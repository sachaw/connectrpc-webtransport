/** An in-memory WebTransport session speaking the wire protocol. */

export interface Request {
  method: string;
  path: string;
  headers: Headers;
  body: ReadableStream<Uint8Array>;
}

export interface Response {
  status: number;
  headers?: HeadersInit;
  body: Iterable<Uint8Array> | AsyncIterable<Uint8Array>;
}

export type Handler = (req: Request) => Promise<Response>;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function fakeSession(
  handler: Handler,
): WebTransport & { closeNow(): void } {
  const closed = Promise.withResolvers<WebTransportCloseInfo>();
  return {
    ready: Promise.resolve(),
    closed: closed.promise,
    closeNow: () => closed.resolve({ closeCode: 0, reason: "" }),
    close: () => closed.resolve({ closeCode: 0, reason: "" }),
    createBidirectionalStream() {
      const toServer = new TransformStream<Uint8Array, Uint8Array>();
      const toClient = new TransformStream<Uint8Array, Uint8Array>();
      void serve(handler, toServer.readable, toClient.writable).catch(() => {});
      return Promise.resolve({
        readable: toClient.readable,
        writable: toServer.writable,
      } as WebTransportBidirectionalStream);
    },
  } as unknown as WebTransport & { closeNow(): void };
}

async function serve(
  handler: Handler,
  input: ReadableStream<Uint8Array>,
  output: WritableStream<Uint8Array>,
) {
  const reader = input.getReader();
  let buffered: Uint8Array = new Uint8Array(0);
  const fill = async () => {
    const { value, done } = await reader.read();
    if (done) return false;
    buffered = concat(buffered, value);
    return true;
  };
  let end = -1;
  while ((end = indexOfHeadEnd(buffered)) < 0) {
    if (!(await fill())) {
      throw new Error("fake server: request closed before head");
    }
  }
  const [requestLine, ...lines] = decoder.decode(buffered.subarray(0, end))
    .split("\r\n");
  buffered = buffered.subarray(end + 4);
  const [method, path, version] = requestLine.split(" ");
  if (version !== "HTTP/1.1") {
    throw new Error(`fake server: bad request line ${requestLine}`);
  }
  const headers = new Headers();
  for (const line of lines) {
    const colon = line.indexOf(":");
    headers.append(line.slice(0, colon).trim(), line.slice(colon + 1).trim());
  }

  const take = async function* (limit: number) {
    while (limit > 0) {
      if (buffered.byteLength === 0 && !(await fill())) {
        throw new Error("fake server: short body");
      }
      const chunk = buffered.subarray(0, Math.min(buffered.byteLength, limit));
      buffered = buffered.subarray(chunk.byteLength);
      limit -= chunk.byteLength;
      yield chunk;
    }
  };
  const line = async () => {
    let i: number;
    while ((i = buffered.indexOf(10)) < 0) {
      if (!(await fill())) throw new Error("fake server: short chunked body");
    }
    const text = decoder.decode(buffered.subarray(0, i)).trim();
    buffered = buffered.subarray(i + 1);
    return text;
  };
  const dechunk = async function* () {
    for (;;) {
      const size = parseInt(await line(), 16);
      if (size === 0) {
        await line();
        return;
      }
      yield* take(size);
      await line();
    }
  };
  const body = headers.get("transfer-encoding") === "chunked"
    ? dechunk()
    : take(Number(headers.get("content-length") ?? 0));

  const response = await handler({
    method,
    path,
    headers,
    body: readable(body),
  });
  const writer = output.getWriter();
  const responseHeaders = new Headers(response.headers);
  const chunked = !responseHeaders.has("content-length");
  if (chunked) responseHeaders.set("transfer-encoding", "chunked");
  let head = `HTTP/1.1 ${response.status} X\r\n`;
  for (const [k, v] of responseHeaders) head += `${k}: ${v}\r\n`;
  await writer.write(encoder.encode(head + "\r\n"));
  for await (const chunk of response.body) {
    if (chunked) {
      await writer.write(
        encoder.encode(`${chunk.byteLength.toString(16)}\r\n`),
      );
    }
    await writer.write(chunk);
    if (chunked) await writer.write(encoder.encode("\r\n"));
  }
  if (chunked) await writer.write(encoder.encode("0\r\n\r\n"));
  await writer.close();
}

// Pulls one chunk ahead, as a real stream's flow-control window would, so a handler that ignores its body does not stall the client.
function readable(it: AsyncIterator<Uint8Array>): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        const { value, done } = await it.next();
        if (done) controller.close();
        else controller.enqueue(value);
      },
    },
    { highWaterMark: 1 },
  );
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

export function envelope(flags: number, data: Uint8Array | string): Uint8Array {
  const bytes = typeof data === "string" ? encoder.encode(data) : data;
  const out = new Uint8Array(5 + bytes.byteLength);
  out[0] = flags;
  new DataView(out.buffer).setUint32(1, bytes.byteLength);
  out.set(bytes, 5);
  return out;
}

export async function* envelopes(
  body: ReadableStream<Uint8Array>,
): AsyncIterable<Uint8Array> {
  let buffered: Uint8Array = new Uint8Array(0);
  for await (const chunk of body) {
    buffered = concat(buffered, chunk);
    while (buffered.byteLength >= 5) {
      const length = new DataView(buffered.slice(0, 5).buffer).getUint32(1);
      if (buffered.byteLength < 5 + length) break;
      yield buffered.subarray(5, 5 + length);
      buffered = buffered.subarray(5 + length);
    }
  }
}

export async function collect(
  body: AsyncIterable<Uint8Array>,
): Promise<Uint8Array> {
  let out: Uint8Array = new Uint8Array(0);
  for await (const chunk of body) out = concat(out, chunk);
  return out;
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.byteLength + b.byteLength);
  out.set(a);
  out.set(b, a.byteLength);
  return out;
}
