//! Serve a `tower::Service` over WebTransport.

use std::convert::Infallible;
use std::io;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioTimer;
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};
use wtransport::endpoint::endpoint_side::Server;
use wtransport::error::ConnectionError;
use wtransport::{Connection, Endpoint, RecvStream, SendStream, VarInt};

use crate::{Body, stream_io};

/// Accept sessions on `endpoint` and serve their RPCs with `service` until `cancel`.
pub async fn serve<S, B>(endpoint: &Endpoint<Server>, service: S, cancel: CancellationToken)
where
    S: Service<http::Request<Body>, Response = http::Response<B>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display + Send,
{
    tracing::debug!(addr = ?endpoint.local_addr().ok(), "accepting WebTransport sessions");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            incoming = endpoint.accept() => {
                let service = service.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    let connection = match incoming.await {
                        Ok(request) => request.accept().await,
                        Err(e) => Err(e),
                    };
                    let reason = match connection {
                        Ok(connection) => serve_session(connection, service, cancel).await,
                        Err(e) => e,
                    };
                    tracing::debug!(%reason, "session ended");
                });
            }
        }
    }
}

/// Serve one session until it ends, returning why. `cancel` closes it (`LocallyClosed`).
pub async fn serve_session<S, B>(
    connection: Connection,
    service: S,
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
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                connection.close(VarInt::from_u32(0), b"");
                return ConnectionError::LocallyClosed;
            }
            bi = connection.accept_bi() => {
                let (send, recv) = match bi {
                    Ok(streams) => streams,
                    Err(reason) => return reason,
                };
                let service = service.clone();
                tokio::spawn(async move {
                    if let Err(e) = call(send, recv, service).await {
                        tracing::debug!(error = %e, "call ended");
                    }
                });
            }
        }
    }
}

async fn call<S, B>(send: SendStream, recv: RecvStream, service: S) -> hyper::Result<()>
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
        .keep_alive(false)
        .half_close(true)
        .serve_connection(
            stream_io(send, recv),
            service_fn(move |request: http::Request<Incoming>| {
                let service = service.clone();
                async move {
                    let request = request.map(|b| b.map_err(io::Error::other).boxed());
                    let response = service
                        .oneshot(request)
                        .await
                        .unwrap_or_else(|e| match e {});
                    Ok::<_, Infallible>(
                        response.map(|b| b.map_err(|e| io::Error::other(e.to_string()))),
                    )
                }
            }),
        )
        .await
}
