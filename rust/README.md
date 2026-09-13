# connectrpc-webtransport

[![crates.io](https://img.shields.io/crates/v/connectrpc-webtransport)](https://crates.io/crates/connectrpc-webtransport)

Connect RPC over WebTransport (HTTP/3). Each QUIC bidi stream carries one
HTTP/1.1 exchange; see [`PROTOCOL.md`](../PROTOCOL.md).

## Server

```rust,ignore
let endpoint = wtransport::Endpoint::server(config)?;
connectrpc_webtransport::server::serve(&endpoint, router.into_axum_service(), cancel).await;
```

Any `tower::Service<http::Request<Body>>` works. Use `serve_session` to serve
a session you accepted yourself.

## Client

```rust,ignore
let transport = connectrpc_webtransport::client::connect(
    "https://peer.example:4433/connect",
    wtransport::ClientConfig::default(),
).await?;
let client = FooServiceClient::builder(transport).build();
```

`client::reconnect(url, config, options)` opens on the first RPC and re-opens
with backoff when the session drops (`on_state` reports transitions);
`Transport::reconnecting(open, options)` does the same over your own factory;
`Transport::new(connection)` wraps a session as-is.
