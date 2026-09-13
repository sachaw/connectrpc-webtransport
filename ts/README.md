# @sachaw/connectrpc-webtransport

[![JSR](https://jsr.io/badges/@sachaw/connectrpc-webtransport)](https://jsr.io/@sachaw/connectrpc-webtransport)

A [Connect](https://connectrpc.com) `Transport` over WebTransport, giving browsers client-streaming and bidi RPCs, and a server for runtimes that can accept WebTransport sessions (Deno).
Codecs, errors, interceptors, limits and generated code are Connect's own.

## Client

```ts
import { createClient } from "@connectrpc/connect";
import { createWebTransportTransport } from "@sachaw/connectrpc-webtransport";

const transport = createWebTransportTransport({
  session: async () => {
    const wt = new WebTransport("https://node.example:4433/connect");
    await wt.ready;
    return wt;
  },
});
const client = createClient(MyService, transport);
```

A factory session is opened on the first RPC and re-opened with jittered backoff when it closes; `onState` reports the transitions and `signal` closes the session for good.
Pass a `WebTransport` instance instead to use one session as-is.

Options mirror `createConnectTransport` (`useBinaryFormat`, `interceptors`, `jsonOptions`, `binaryOptions`, `readMaxBytes`, `writeMaxBytes`, `defaultTimeoutMs`, `acceptCompression`, `sendCompression`, `compressMinBytes`); `compressionGzip` is provided.
`useBinaryFormat` defaults to `true` here.
`sendOrder` sets a per-method QUIC send priority.

## Server

```ts
import { createWebTransportServer } from "@sachaw/connectrpc-webtransport";

const serve = createWebTransportServer({
  routes: (router) => router.service(MyService, impl),
});

const listener = new Deno.QuicEndpoint({ port: 4433 }).listen({
  cert,
  key,
  alpnProtocols: ["h3"],
});
for await (const incoming of listener) {
  void Deno.upgradeWebTransport(await incoming.accept()).then(serve);
}
```

`routes` and the remaining options are those of Connect's `createConnectRouter`; `contextValues` runs once per session, so handlers can see which peer they serve, and `onSession` reports sessions opening and closing.
A call's `signal` aborts when the client cancels it or the session ends.
The server takes accepted sessions; listening and TLS are the runtime's.

Concurrent calls per session are bounded by QUIC's stream limit (typically 100); beyond it, opening a stream waits.
Raise the listener's bidirectional-stream limit if a client runs more than that at once.

The Rust counterpart is the [`connectrpc-webtransport`](https://crates.io/crates/connectrpc-webtransport) crate; the wire format is in [`PROTOCOL.md`](https://github.com/sachaw/connectrpc-webtransport/blob/master/PROTOCOL.md).
