import {
  type ConnectRouter,
  type ContextValues,
  createConnectRouter,
} from "@connectrpc/connect";
import {
  type UniversalHandler,
  type UniversalHandlerOptions,
  type UniversalServerResponse,
  uResponseMethodNotAllowed,
  uResponseNotFound,
} from "@connectrpc/connect/protocol";
import { encodeChunk, encodeHead, Http1Reader, LAST_CHUNK } from "./http1.ts";

export interface WebTransportServerOptions
  extends Omit<Partial<UniversalHandlerOptions>, "contextValues"> {
  routes: (router: ConnectRouter) => void;
  /** Context values for every call on a session, e.g. its peer identity. */
  contextValues?: (session: WebTransport) => ContextValues;
  /** Time allowed for a request head to arrive on a new stream. */
  headTimeoutMs?: number;
  /** Session lifecycle; per-call observability belongs in Connect interceptors. */
  onSession?: (session: WebTransport, event: "open" | "close") => void;
}

/** Serves each bidi stream of a session as one HTTP/1.1 exchange, until the session closes. */
export type WebTransportServer = (session: WebTransport) => Promise<void>;

export function createWebTransportServer(
  options: WebTransportServerOptions,
): WebTransportServer {
  const {
    routes,
    contextValues,
    headTimeoutMs = 30_000,
    onSession,
    ...handlerOptions
  } = options;
  const router = createConnectRouter({
    ...handlerOptions,
    connect: true,
    grpc: false,
    grpcWeb: false,
  });
  routes(router);
  const handlers = new Map(router.handlers.map((h) => [h.requestPath, h]));

  return async (session) => {
    const closed = new AbortController();
    onSession?.(session, "open");
    session.closed.then(() => closed.abort(), (e) => closed.abort(e)).finally(
      () => onSession?.(session, "close"),
    );
    const context: Session = {
      handlers,
      signal: closed.signal,
      contextValues: contextValues?.(session),
    };
    const streams = session.incomingBidirectionalStreams.getReader();
    for (;;) {
      let next: ReadableStreamReadResult<WebTransportBidirectionalStream>;
      try {
        next = await streams.read();
      } catch {
        return;
      }
      if (next.done) return;
      void exchange(next.value, context, headTimeoutMs).catch(() => {});
    }
  };
}

interface Session {
  handlers: Map<string, UniversalHandler>;
  signal: AbortSignal;
  contextValues?: ContextValues;
}

async function exchange(
  stream: WebTransportBidirectionalStream,
  session: Session,
  headTimeoutMs: number,
) {
  const reader = new Http1Reader(stream.readable.getReader());
  const writer = stream.writable.getWriter();
  try {
    await respondTo(reader, writer, session, headTimeoutMs);
  } finally {
    // An unread request tail would otherwise pin the stream.
    await reader.cancel().catch(() => {});
    await writer.abort().catch(() => {});
  }
}

async function respondTo(
  reader: Http1Reader,
  writer: WritableStreamDefaultWriter<Uint8Array>,
  session: Session,
  headTimeoutMs: number,
) {
  const reset = new AbortController();
  writer.closed.catch((e) => reset.abort(e));
  const signal = AbortSignal.any([session.signal, reset.signal]);
  const respond = async (res: UniversalServerResponse) => {
    const headers = new Headers(res.header);
    headers.delete("transfer-encoding");
    headers.set(
      res.body ? "transfer-encoding" : "content-length",
      res.body ? "chunked" : "0",
    );
    await writer.write(encodeHead(`HTTP/1.1 ${res.status} `, headers));
    if (res.body) {
      for await (const chunk of res.body) {
        if (chunk.byteLength > 0) await writer.write(encodeChunk(chunk));
      }
      await writer.write(LAST_CHUNK);
    }
    await writer.close();
  };

  const timeout = setTimeout(
    () => reader.cancel().catch(() => {}),
    headTimeoutMs,
  );
  let head;
  try {
    head = await reader.head();
  } catch {
    return respond({ status: 400 });
  } finally {
    clearTimeout(timeout);
  }
  if (head === null) return writer.close();

  const [method, target, version] = head.startLine.split(" ");
  if (version !== "HTTP/1.1") return respond({ status: 400 });
  const url = URL.parse(
    target,
    `https://${head.headers.get("host") ?? "localhost"}`,
  );
  if (!url) return respond({ status: 400 });
  const handler = session.handlers.get(url.pathname);
  if (!handler) return respond(uResponseNotFound);
  if (!handler.allowedMethods.includes(method)) {
    return respond(uResponseMethodNotAllowed);
  }
  // The exchange rides a QUIC stream inside HTTP/3; Connect only asks this to refuse bidi over HTTP/1.x connections.
  return respond(
    await handler({
      httpVersion: "3",
      method,
      url: url.href,
      header: head.headers,
      body: reader.body(head.headers, "request"),
      signal,
      contextValues: session.contextValues,
    }),
  );
}
