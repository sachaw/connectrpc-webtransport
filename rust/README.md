# connectrpc-webtransport

[![crates.io](https://img.shields.io/crates/v/connectrpc-webtransport)](https://crates.io/crates/connectrpc-webtransport)

Connect RPC over WebTransport (HTTP/3).
Each QUIC bidi stream carries one HTTP/1.1 exchange; see [`PROTOCOL.md`](../PROTOCOL.md).

## Server

```rust,ignore
let endpoint = wtransport::Endpoint::server(config)?;
connectrpc_webtransport::server::serve(&endpoint, router.into_axum_service(), ServeOptions::default(), cancel).await;
```

Any `tower::Service<http::Request<Body>>` works; `serve_session` serves a session you accepted yourself.
On `cancel`, in-flight calls get `ServeOptions::drain` to finish before their sessions are closed.
Each request carries the `wtransport::Connection` it arrived on in its extensions, so a layer can read `remote_address()` or `peer_identity()`.

Concurrent calls per session are bounded by QUIC's stream limit (quinn's default is 100); beyond it, opening a stream waits.
Raise `max_concurrent_bidi_streams` in the server's transport config if a client runs more calls than that at once.

## Client

```rust,ignore
let transport = connectrpc_webtransport::client::connect(
    "https://peer.example:4433/connect",
    wtransport::ClientConfig::default(),
).await?;
let client = FooServiceClient::builder(transport).build();
```

`client::reconnect(url, config, options)` opens on the first RPC and re-opens with jittered backoff when the session drops (`on_state` reports transitions); `Transport::reconnecting(open, options)` does the same over your own factory; `Transport::new(connection)` wraps a session as-is.
`Transport::close()`, or dropping the last handle, ends the session and stops redialling; `with_priority` sets a per-call QUIC send priority.

| feature | default | |
|---|---|---|
| `server` | ✓ | `server` module |
| `client` | ✓ | `client` module |

MPL-2.0.
