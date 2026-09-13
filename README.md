# connectrpc-webtransport

[![crates.io](https://img.shields.io/crates/v/connectrpc-webtransport)](https://crates.io/crates/connectrpc-webtransport) [![JSR](https://jsr.io/badges/@sachaw/connectrpc-webtransport)](https://jsr.io/@sachaw/connectrpc-webtransport)

[Connect RPC](https://connectrpc.com) over WebTransport (HTTP/3): each QUIC bidi stream carries one HTTP/1.1 exchange, giving browsers the client-streaming and bidi RPCs that fetch cannot.

| | |
|---|---|
| [`rust/`](rust/) | crate `connectrpc-webtransport` — server bridge for any `tower::Service` and a `connectrpc` `ClientTransport` |
| [`ts/`](ts/) | JSR `@sachaw/connectrpc-webtransport` — a `@connectrpc/connect` `Transport`, and a server for Deno |
| [`PROTOCOL.md`](PROTOCOL.md) | the wire format |

`cargo test` in `rust/` also runs each client against the other side's server over real QUIC (needs Deno); `cargo bench` measures unary throughput and bidi bandwidth against both servers, and `deno bench` in `ts/` the TypeScript pair in-process.

MPL-2.0.
