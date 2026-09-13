//! Serve a `tower::Service` over WebTransport.
//!
//! Each request carries the [`Connection`] it arrived on in its extensions, so a layer can read `remote_address()` or `peer_identity()`.

use std::convert::Infallible;
use std::io;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioTimer;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};
use tracing::Instrument;
use wtransport::endpoint::endpoint_side::Server;
use wtransport::error::ConnectionError;
use wtransport::{Connection, Endpoint, RecvStream, SendStream, VarInt};

use crate::{Body, stream_io};

#[derive(Clone, Copy, Debug)]
pub struct ServeOptions {
    /// How long in-flight calls may finish after `cancel` before their sessions are closed.
    pub drain: Duration,
    /// Time allowed for a request head to arrive on a new stream.
    pub header_timeout: Duration,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            drain: Duration::from_secs(5),
            header_timeout: Duration::from_secs(30),
        }
    }
}

/// Accept sessions on `endpoint` and serve their RPCs with `service` until `cancel`, then drain.
pub async fn serve<S, B>(
    endpoint: &Endpoint<Server>,
    service: S,
    options: ServeOptions,
    cancel: CancellationToken,
) where
    S: Service<http::Request<Body>, Response = http::Response<B>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    tracing::debug!(addr = ?endpoint.local_addr().ok(), "accepting WebTransport sessions");
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
            incoming = endpoint.accept() => {
                let service = service.clone();
                let cancel = cancel.clone();
                sessions.spawn(async move {
                    let connection = match incoming.await {
                        Ok(request) => request.accept().await,
                        Err(e) => Err(e),
                    };
                    let reason = match connection {
                        Ok(connection) => {
                            let span = tracing::debug_span!("session", peer = %connection.remote_address());
                            serve_session(connection, service, options, cancel).instrument(span).await
                        }
                        Err(e) => e,
                    };
                    tracing::debug!(%reason, "session ended");
                });
            }
        }
    }
    while sessions.join_next().await.is_some() {}
}

/// Serve one session until it ends, returning why.
/// On `cancel`, in-flight calls get `options.drain` to finish, then the session is closed (`LocallyClosed`).
pub async fn serve_session<S, B>(
    connection: Connection,
    service: S,
    options: ServeOptions,
    cancel: CancellationToken,
) -> ConnectionError
where
    S: Service<http::Request<Body>, Response = http::Response<B>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    let mut calls = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            Some(_) = calls.join_next(), if !calls.is_empty() => {}
            bi = connection.accept_bi() => {
                let (send, recv) = match bi {
                    Ok(streams) => streams,
                    Err(reason) => return reason,
                };
                let (service, connection) = (service.clone(), connection.clone());
                calls.spawn(async move {
                    if let Err(e) = call(send, recv, service, connection, options.header_timeout).await {
                        tracing::debug!(error = %e, "call ended");
                    }
                });
            }
        }
    }
    let _ = tokio::time::timeout(options.drain, async {
        while calls.join_next().await.is_some() {}
    })
    .await;
    connection.close(VarInt::from_u32(0), b"");
    ConnectionError::LocallyClosed
}

async fn call<S, B>(
    send: SendStream,
    recv: RecvStream,
    service: S,
    connection: Connection,
    header_timeout: Duration,
) -> hyper::Result<()>
where
    S: Service<http::Request<Body>, Response = http::Response<B>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout)
        .keep_alive(false)
        .half_close(true)
        .serve_connection(
            stream_io(send, recv),
            service_fn(move |request: http::Request<Incoming>| {
                let (service, connection) = (service.clone(), connection.clone());
                let span = tracing::debug_span!("call", path = %request.uri().path());
                async move {
                    let mut request = request.map(|b| b.map_err(io::Error::other).boxed());
                    request.extensions_mut().insert(connection);
                    let response = service
                        .oneshot(request)
                        .await
                        .unwrap_or_else(|e| match e {});
                    tracing::debug!(status = response.status().as_u16(), "responded");
                    Ok::<_, Infallible>(
                        response.map(|b| b.map_err(|e| io::Error::other(e.to_string()))),
                    )
                }
                .instrument(span)
            }),
        )
        .await
}
