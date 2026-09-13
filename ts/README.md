# @sachaw/connectrpc-webtransport

[![JSR](https://jsr.io/badges/@sachaw/connectrpc-webtransport)](https://jsr.io/@sachaw/connectrpc-webtransport)

A [Connect](https://connectrpc.com) `Transport` over WebTransport, giving
browsers client-streaming and bidi RPCs. Codecs, errors, interceptors, limits
and generated clients are Connect's own.

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

A factory session is opened on the first RPC and re-opened with backoff when it
closes; `onState` reports the transitions. Pass a `WebTransport` instance
instead to use one session as-is.

Options mirror `createConnectTransport` (`useBinaryFormat`, `interceptors`,
`jsonOptions`, `binaryOptions`, `readMaxBytes`, `writeMaxBytes`,
`defaultTimeoutMs`). `useBinaryFormat` defaults to `true` here.

The server is the
[`connectrpc-webtransport`](https://crates.io/crates/connectrpc-webtransport)
crate; the wire format is in
[`PROTOCOL.md`](https://github.com/sachaw/connectrpc-webtransport/blob/master/PROTOCOL.md).
