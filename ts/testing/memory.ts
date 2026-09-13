/** Two linked in-memory WebTransport sessions: streams opened on `client` arrive on `server`. */
export function memorySessionPair(): {
  client: WebTransport;
  server: WebTransport;
} {
  const closed = Promise.withResolvers<WebTransportCloseInfo>();
  let incoming!: ReadableStreamDefaultController<
    WebTransportBidirectionalStream
  >;
  const incomingBidirectionalStreams = new ReadableStream<
    WebTransportBidirectionalStream
  >({
    start: (controller) => void (incoming = controller),
  });
  const close = () => {
    closed.resolve({ closeCode: 0, reason: "" });
    try {
      incoming.close();
    } catch { /* already closed */ }
  };
  // Queued chunks stand in for a flow-control window, so a peer that is not reading yet does not stall writes.
  const pipe = () =>
    new TransformStream<Uint8Array, Uint8Array>({}, {}, { highWaterMark: 64 });
  const common = { ready: Promise.resolve(), closed: closed.promise, close };
  const client = {
    ...common,
    createBidirectionalStream() {
      const toServer = pipe();
      const toClient = pipe();
      incoming.enqueue(bidi(toServer.readable, toClient.writable));
      return Promise.resolve(bidi(toClient.readable, toServer.writable));
    },
  } as unknown as WebTransport;
  const server = {
    ...common,
    incomingBidirectionalStreams,
  } as unknown as WebTransport;
  return { client, server };
}

function bidi(
  readable: ReadableStream<Uint8Array>,
  writable: WritableStream<Uint8Array>,
): WebTransportBidirectionalStream {
  return { readable, writable } as unknown as WebTransportBidirectionalStream;
}
