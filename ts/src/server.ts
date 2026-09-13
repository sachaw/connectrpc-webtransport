import { type ConnectRouter, createConnectRouter } from "@connectrpc/connect";
import {
  type UniversalHandler,
  type UniversalHandlerOptions,
  type UniversalServerResponse,
  uResponseMethodNotAllowed,
  uResponseNotFound,
} from "@connectrpc/connect/protocol";
import { encodeChunk, encodeHead, Http1Reader, LAST_CHUNK } from "./http1.ts";

export interface WebTransportServerOptions
  extends Partial<UniversalHandlerOptions> {
  routes: (router: ConnectRouter) => void;
  /** Time allowed for a request head to arrive on a new stream. */
  headTimeoutMs?: number;
}

/** Serves each bidi stream of a session as one HTTP/1.1 exchange, until the session closes. */
export type WebTransportServer = (session: WebTransport) => Promise<void>;

export function createWebTransportServer(
  options: WebTransportServerOptions,
): WebTransportServer {
  const { routes, headTimeoutMs = 30_000, ...handlerOptions } = options;
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
    session.closed.then(() => closed.abort(), (e) => closed.abort(e));
    const streams = session.incomingBidirectionalStreams.getReader();
    for (;;) {
      let next: ReadableStreamReadResult<WebTransportBidirectionalStream>;
      try {
        next = await streams.read();
      } catch {
        return;
      }
      if (next.done) return;
      void exchange(next.value, handlers, closed.signal, headTimeoutMs).catch(
        () => {},
      );
    }
  };
}

async function exchange(
  stream: WebTransportBidirectionalStream,
  handlers: Map<string, UniversalHandler>,
  signal: AbortSignal,
  headTimeoutMs: number,
) {
  const reader = new Http1Reader(stream.readable.getReader());
  const writer = stream.writable.getWriter();
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
  const url = new URL(
    target,
    `https://${head.headers.get("host") ?? "localhost"}`,
  );
  const handler = handlers.get(url.pathname);
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
    }),
  );
}
