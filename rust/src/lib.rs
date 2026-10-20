//! Connect RPC over WebTransport (HTTP/3): each bidi stream carries one HTTP/1.1 exchange (see `PROTOCOL.md`).
//!
//! - [`server`] — serve any `tower::Service`, e.g. a `connectrpc` router.
//! - [`client`] — a `connectrpc` [`ClientTransport`](connectrpc::client::ClientTransport).

#![cfg_attr(docsrs, feature(doc_cfg))]

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
mod stream;

/// The stream error code a call abandoned before it completes is reset and stopped with: Connect's `canceled`.
pub const CANCELED: u32 = 1;

/// How the peer abandoned a call, if it has: the stream error code it stopped the call with.
///
/// The server puts one in every request's extensions. A request the client sends that carries one
/// is abandoned with the same code, so a proxy that copies it onto the request it forwards passes
/// the peer's reason upstream; without one, or before the peer says, the code is [`CANCELED`].
#[derive(Clone, Debug, Default)]
pub struct Abandonment(Arc<AtomicU64>);

impl Abandonment {
    /// The code the peer abandoned the call with; `None` while it has not.
    pub fn code(&self) -> Option<u32> {
        self.0
            .load(Ordering::Acquire)
            .checked_sub(1)
            .map(|c| c as u32)
    }

    #[cfg(feature = "server")]
    pub(crate) fn abandon(&self, code: u32) {
        self.0.store(u64::from(code) + 1, Ordering::Release);
    }
}
