import {
  appendHeaders,
  Code,
  ConnectError,
  createContextValues,
  type Interceptor,
  type Transport,
} from "@connectrpc/connect";
import {
  createAsyncIterable,
  createMethodSerializationLookup,
  pipe,
  pipeTo,
  runStreamingCall,
  runUnaryCall,
  sinkAllBytes,
  transformJoinEnvelopes,
  transformParseEnvelope,
  transformSerializeEnvelope,
  transformSplitEnvelope,
} from "@connectrpc/connect/protocol";
import {
  createEndStreamSerialization,
  endStreamFlag,
  errorFromJsonBytes,
  requestHeader,
  trailerDemux,
  validateResponse,
} from "@connectrpc/connect/protocol-connect";
import type {
  BinaryReadOptions,
  BinaryWriteOptions,
  DescMethod,
  JsonReadOptions,
  JsonWriteOptions,
} from "@bufbuild/protobuf";

export type LinkPhase = "connecting" | "connected" | "reconnecting";

export interface LinkState {
  phase: LinkPhase;
  /** Consecutive failed opens; 0 while connected. */
  failures: number;
  /** Epoch ms of the next attempt; 0 when none is pending. */
  nextAttemptAt: number;
}

export interface WebTransportTransportOptions {
  /** A ready session, or a factory: opened on the first RPC, re-opened with backoff when it closes. */
  session: WebTransport | (() => Promise<WebTransport>);
  /** Binary protobuf (default) or JSON. Connect's HTTP transports default to JSON. */
  useBinaryFormat?: boolean;
  interceptors?: Interceptor[];
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;
  readMaxBytes?: number;
  writeMaxBytes?: number;
  defaultTimeoutMs?: number;
  /** Delay between failed opens, doubling from `initialMs` up to `maxMs`. */
  backoff?: { initialMs: number; maxMs: number };
  /** Called on every session transition. Factory sessions only. */
  onState?: (state: LinkState) => void;
}

const MAX_BYTES = 0xffffffff;
const MAX_HEAD_BYTES = 1 << 20;

export function createWebTransportTransport(
  options: WebTransportTransportOptions,
): Transport {
  const useBinaryFormat = options.useBinaryFormat ?? true;
  const limits = {
    readMaxBytes: options.readMaxBytes ?? MAX_BYTES,
    writeMaxBytes: options.writeMaxBytes ?? MAX_BYTES,
  };
  const source = typeof options.session === "function"
    ? reconnecting(options.session, options)
    : () => Promise.resolve(options.session as WebTransport);
  const session = async () => {
    try {
      return await source();
    } catch (e) {
      throw ConnectError.from(e, Code.Unavailable);
    }
  };
  const timeout = (ms: number | undefined) =>
    ms === undefined ? options.defaultTimeoutMs : ms <= 0 ? undefined : ms;

  return {
    async unary(method, signal, timeoutMs, header, message, contextValues) {
      const serialization = createMethodSerializationLookup(
        method,
        options.binaryOptions,
        options.jsonOptions,
        limits,
      );
      timeoutMs = timeout(timeoutMs);
      return await runUnaryCall({
        interceptors: options.interceptors,
        signal,
        timeoutMs,
        req: {
          stream: false,
          service: method.parent,
          method,
          requestMethod: "POST",
          url: path(method),
          header: requestHeader(
            method.methodKind,
            useBinaryFormat,
            timeoutMs,
            header,
            false,
          ),
          contextValues: contextValues ?? createContextValues(),
          message,
        },
        next: async (req) => {
          const body = serialization.getI(useBinaryFormat).serialize(
            req.message,
          );
          const call = await open(
            await session(),
            req.url,
            req.header,
            req.signal,
            body.byteLength,
          );
          await call.send(createAsyncIterable([body]));
          const res = await call.response();
          const { isUnaryError, unaryError } = validateResponse(
            method.methodKind,
            useBinaryFormat,
            res.status,
            res.header,
          );
          const [resHeader, trailer] = trailerDemux(res.header);
          const bytes = await pipeTo(
            res.body,
            sinkAllBytes(limits.readMaxBytes, res.header.get("content-length")),
            { propagateDownStreamError: false },
          );
          if (isUnaryError) {
            throw errorFromJsonBytes(
              bytes,
              appendHeaders(resHeader, trailer),
              unaryError,
            );
          }
          return {
            stream: false,
            service: method.parent,
            method,
            header: resHeader,
            trailer,
            message: serialization.getO(useBinaryFormat).parse(bytes),
          };
        },
      });
    },

    async stream(method, signal, timeoutMs, header, input, contextValues) {
      const serialization = createMethodSerializationLookup(
        method,
        options.binaryOptions,
        options.jsonOptions,
        limits,
      );
      const endStreamSerialization = createEndStreamSerialization(
        options.jsonOptions,
      );
      timeoutMs = timeout(timeoutMs);
      return await runStreamingCall({
        interceptors: options.interceptors,
        signal,
        timeoutMs,
        req: {
          stream: true,
          service: method.parent,
          method,
          requestMethod: "POST",
          url: path(method),
          header: requestHeader(
            method.methodKind,
            useBinaryFormat,
            timeoutMs,
            header,
            false,
          ),
          contextValues: contextValues ?? createContextValues(),
          message: input,
        },
        next: async (req) => {
          const body = pipe(
            req.message,
            transformSerializeEnvelope(serialization.getI(useBinaryFormat)),
            transformJoinEnvelopes(),
            { propagateDownStreamError: true },
          );
          const wt = await session();
          let call: Call;
          if (method.methodKind === "server_streaming") {
            const bytes = await pipeTo(body, sinkAllBytes(MAX_BYTES), {
              propagateDownStreamError: true,
            });
            call = await open(
              wt,
              req.url,
              req.header,
              req.signal,
              bytes.byteLength,
            );
            await call.send(createAsyncIterable([bytes]));
          } else {
            call = await open(wt, req.url, req.header, req.signal);
            void call.send(body).catch(() => {});
          }
          const res = await call.response();
          validateResponse(
            method.methodKind,
            useBinaryFormat,
            res.status,
            res.header,
          );
          const trailer = new Headers();
          const message = pipe(
            res.body,
            transformSplitEnvelope(limits.readMaxBytes),
            transformParseEnvelope(
              serialization.getO(useBinaryFormat),
              endStreamFlag,
              endStreamSerialization,
            ),
            async function* (envelopes) {
              let ended = false;
              for await (const envelope of envelopes) {
                if (envelope.end) {
                  if (ended) {
                    throw new ConnectError(
                      "extra EndStreamResponse",
                      Code.InvalidArgument,
                    );
                  }
                  ended = true;
                  if (envelope.value.error) {
                    res.header.forEach((v, k) =>
                      envelope.value.error!.metadata.append(k, v)
                    );
                    throw envelope.value.error;
                  }
                  envelope.value.metadata.forEach((v, k) => trailer.set(k, v));
                  continue;
                }
                if (ended) {
                  throw new ConnectError(
                    "message after EndStreamResponse",
                    Code.InvalidArgument,
                  );
                }
                yield envelope.value;
              }
              if (!ended) {
                throw new ConnectError(
                  "missing EndStreamResponse",
                  Code.InvalidArgument,
                );
              }
            },
            { propagateDownStreamError: true },
          );
          return { ...req, header: res.header, trailer, message };
        },
      });
    },
  };
}

function path(method: DescMethod): string {
  return `/${method.parent.typeName}/${method.name}`;
}

interface Call {
  /** Writes the body and half-closes. Returns early if the peer stops reading; a failing body aborts the stream. */
  send(body: AsyncIterable<Uint8Array>): Promise<void>;
  response(): Promise<
    { status: number; header: Headers; body: AsyncIterable<Uint8Array> }
  >;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const CRLF = encoder.encode("\r\n");
const LAST_CHUNK = encoder.encode("0\r\n\r\n");

// A known-length request carries content-length; otherwise it is chunked, so its end never depends on the stream's FIN (Deno never sends one).
async function open(
  session: WebTransport,
  path: string,
  header: Headers,
  signal: AbortSignal | undefined,
  contentLength?: number,
): Promise<Call> {
  const stream = await session.createBidirectionalStream().catch((e) => {
    throw ConnectError.from(e, Code.Unavailable);
  });
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();
  let aborted: unknown;
  const abort = (reason: unknown) => {
    aborted = reason;
    writer.abort(reason).catch(() => {});
    reader.cancel(reason).catch(() => {});
  };
  if (signal?.aborted) abort(signal.reason);
  signal?.addEventListener("abort", () => abort(signal.reason), { once: true });

  const chunked = contentLength === undefined;
  header.delete("transfer-encoding");
  if (chunked) header.set("transfer-encoding", "chunked");
  else header.set("content-length", String(contentLength));
  let head = `POST ${path} HTTP/1.1\r\n`;
  for (const [name, value] of header) head += `${name}: ${value}\r\n`;
  await writer.write(encoder.encode(head + "\r\n"));

  let buffered: Uint8Array = new Uint8Array(0);
  const fill = async (): Promise<boolean> => {
    const { value, done } = await reader.read();
    if (done) return false;
    buffered = concat(buffered, value);
    return true;
  };
  async function* rest(limit: number): AsyncIterable<Uint8Array> {
    while (limit > 0) {
      if (buffered.byteLength === 0 && !(await fill())) {
        if (aborted !== undefined) throw aborted;
        if (limit === Infinity) return;
        throw new ConnectError(
          "stream ended inside the response body",
          Code.Unavailable,
        );
      }
      const chunk = buffered.subarray(0, Math.min(buffered.byteLength, limit));
      buffered = buffered.subarray(chunk.byteLength);
      limit -= chunk.byteLength;
      if (chunk.byteLength > 0) yield chunk;
    }
  }
  const line = async (): Promise<string> => {
    let i: number;
    while ((i = buffered.indexOf(10)) < 0) {
      if (!(await fill())) {
        throw aborted ??
          new ConnectError(
            "stream ended inside a chunked body",
            Code.Unavailable,
          );
      }
    }
    const text = decoder.decode(buffered.subarray(0, i)).trim();
    buffered = buffered.subarray(i + 1);
    return text;
  };
  // RFC 9112 §7.1; trailers are dropped (Connect does not use them).
  async function* dechunked(): AsyncIterable<Uint8Array> {
    for (;;) {
      const size = parseInt(await line(), 16);
      if (!Number.isInteger(size)) {
        throw new ConnectError("invalid chunk size", Code.Internal);
      }
      if (size === 0) break;
      yield* rest(size);
      await line();
    }
    while ((await line()) !== "");
  }

  return {
    async send(body) {
      try {
        for await (const chunk of body) {
          if (chunk.byteLength === 0) continue;
          const frame = chunked
            ? concat(
              concat(
                encoder.encode(`${chunk.byteLength.toString(16)}\r\n`),
                chunk,
              ),
              CRLF,
            )
            : chunk;
          try {
            await writer.write(frame);
          } catch {
            return;
          }
        }
        if (chunked) await writer.write(LAST_CHUNK).catch(() => {});
        await writer.close().catch(() => {});
      } catch (e) {
        abort(e);
        throw e;
      }
    },
    async response() {
      let end = -1;
      while ((end = indexOfHeadEnd(buffered)) < 0) {
        if (buffered.byteLength > MAX_HEAD_BYTES) {
          throw new ConnectError(
            "response head exceeds the size limit",
            Code.ResourceExhausted,
          );
        }
        if (!(await fill())) {
          throw aborted ??
            new ConnectError(
              "stream closed before the response head",
              Code.Unavailable,
            );
        }
      }
      const [statusLine, ...lines] = decoder.decode(buffered.subarray(0, end))
        .split("\r\n");
      buffered = buffered.subarray(end + 4);
      const status = Number(statusLine.split(" ")[1]);
      if (!statusLine.startsWith("HTTP/1.1 ") || !Number.isInteger(status)) {
        throw new ConnectError(
          `invalid status line: ${statusLine}`,
          Code.Internal,
        );
      }
      const header = new Headers();
      for (const line of lines) {
        const colon = line.indexOf(":");
        header.append(
          line.slice(0, colon).trim(),
          line.slice(colon + 1).trim(),
        );
      }
      const chunked =
        header.get("transfer-encoding")?.toLowerCase() === "chunked";
      const length = header.get("content-length");
      const body = chunked
        ? dechunked()
        : rest(length === null ? Infinity : Number(length));
      return { status, header, body };
    },
  };
}

function indexOfHeadEnd(buf: Uint8Array): number {
  for (let i = 0; i + 3 < buf.byteLength; i++) {
    if (
      buf[i] === 13 && buf[i + 1] === 10 && buf[i + 2] === 13 &&
      buf[i + 3] === 10
    ) return i;
  }
  return -1;
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.byteLength + b.byteLength);
  out.set(a);
  out.set(b, a.byteLength);
  return out;
}

function reconnecting(
  open: () => Promise<WebTransport>,
  options: Pick<WebTransportTransportOptions, "backoff" | "onState">,
): () => Promise<WebTransport> {
  const { initialMs, maxMs } = options.backoff ??
    { initialMs: 500, maxMs: 5_000 };
  let current: Promise<WebTransport> | null = null;
  let failures = 0;
  let nextAttemptAt = 0;
  let everConnected = false;
  const emit = (phase: LinkPhase) =>
    options.onState?.({ phase, failures, nextAttemptAt });
  const down = (): LinkPhase => everConnected ? "reconnecting" : "connecting";

  const dial = (): Promise<WebTransport> => {
    const wait = Math.max(0, nextAttemptAt - Date.now());
    const attempt: Promise<WebTransport> = new Promise<void>((r) =>
      setTimeout(r, wait)
    )
      .then(open)
      .then(
        (session) => {
          failures = 0;
          nextAttemptAt = 0;
          everConnected = true;
          const drop = () => {
            if (current !== attempt) return;
            current = null;
            emit(down());
          };
          session.closed.then(drop, drop);
          emit("connected");
          return session;
        },
        (e) => {
          failures += 1;
          nextAttemptAt = Date.now() +
            Math.min(maxMs, initialMs * 2 ** (failures - 1));
          if (current === attempt) current = null;
          emit(down());
          throw e;
        },
      );
    return attempt;
  };

  return () => (current ??= dial());
}
