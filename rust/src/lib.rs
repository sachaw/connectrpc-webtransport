//! Connect RPC over WebTransport (HTTP/3): each bidi stream carries one HTTP/1.1 exchange (see `PROTOCOL.md`).
//!
//! - [`server`] — serve any `tower::Service`, e.g. a `connectrpc` router.
//! - [`client`] — a `connectrpc` [`ClientTransport`](connectrpc::client::ClientTransport).

#![cfg_attr(docsrs, feature(doc_cfg))]

use std::io;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;

mod error;

pub use error::Error;

pub type Body = BoxBody<Bytes, io::Error>;

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub mod client;
#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub mod server;

#[cfg(any(feature = "server", feature = "client"))]
fn stream_io(
    send: wtransport::SendStream,
    recv: wtransport::RecvStream,
) -> hyper_util::rt::TokioIo<tokio::io::Join<wtransport::RecvStream, wtransport::SendStream>> {
    hyper_util::rt::TokioIo::new(tokio::io::join(recv, send))
}
