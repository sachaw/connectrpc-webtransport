#![cfg(all(feature = "server", feature = "client"))]

//! A call abandoned by either side ends on both at once, however idle it is.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use connectrpc::client::{ClientBody, ClientTransport};
use http_body_util::{BodyExt, Full, StreamBody};
use tokio_util::sync::CancellationToken;
use tower::service_fn;
use wtransport::{ClientConfig, Endpoint, Identity, ServerConfig, VarInt};

use connectrpc_webtransport::{Body, client, server};

/// Counts the calls the server is holding.
struct Held(Arc<AtomicUsize>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Fixture {
    url: String,
    digest: wtransport::tls::Sha256Digest,
    held: Arc<AtomicUsize>,
    cancel: CancellationToken,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Fixture {
    fn config(&self) -> ClientConfig {
        ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([self.digest.clone()])
            .build()
    }

    async fn none_held(&self) {
        let held = self.held.clone();
        tokio::time::timeout(Duration::from_secs(2), async move {
            while held.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{} calls still held", self.held.load(Ordering::SeqCst)));
    }
}

/// `/Watch` answers with a head and then nothing, ever; `/Stall` never answers.
async fn start() -> Fixture {
    let identity = Identity::self_signed_builder()
        .subject_alt_names(["127.0.0.1"])
        .from_now_utc()
        .validity_days(14)
        .build()
        .unwrap();
    let digest = identity.certificate_chain().as_slice()[0].hash();
    let endpoint = Endpoint::server(
        ServerConfig::builder()
            .with_bind_address("127.0.0.1:0".parse().unwrap())
            .with_identity(identity)
            .build(),
    )
    .unwrap();
    let url = format!(
        "https://127.0.0.1:{}/",
        endpoint.local_addr().unwrap().port()
    );
    let held = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let service = {
        let held = held.clone();
        service_fn(move |req: http::Request<Body>| {
            held.fetch_add(1, Ordering::SeqCst);
            let guard = Held(held.clone());
            async move {
                if req.uri().path().ends_with("/Stall") {
                    std::future::pending::<()>().await;
                }
                let idle = futures_util::stream::unfold(guard, |guard| async move {
                    std::future::pending::<()>().await;
                    Some((Ok(http_body::Frame::data(Bytes::new())), guard))
                });
                let body: Body = StreamBody::new(Box::pin(idle)).boxed();
                Ok::<_, Infallible>(http::Response::new(body))
            }
        })
    };
    let serving = cancel.clone();
    tokio::spawn(async move {
        server::serve(&endpoint, service, server::ServeOptions::default(), serving).await
    });
    Fixture {
        url,
        digest,
        held,
        cancel,
    }
}

fn watch(path: &str) -> http::Request<ClientBody> {
    http::Request::post(format!("https://127.0.0.1:1/test.v1.Test/{path}"))
        .body(
            Full::new(Bytes::new())
                .map_err(|e: Infallible| match e {})
                .boxed(),
        )
        .unwrap()
}

#[tokio::test]
async fn dropping_a_response_ends_the_call_on_the_server() {
    let f = start().await;
    let transport = client::connect(&f.url, f.config()).await.unwrap();
    let response = transport.send(watch("Watch")).await.unwrap();
    assert_eq!(f.held.load(Ordering::SeqCst), 1);
    drop(response);
    f.none_held().await;
}

#[tokio::test]
async fn dropping_a_call_before_its_head_ends_it_on_the_server() {
    let f = start().await;
    let transport = client::connect(&f.url, f.config()).await.unwrap();
    let call = transport.send(watch("Stall"));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), call)
            .await
            .is_err()
    );
    f.none_held().await;
}

/// More abandoned calls than the session's concurrent-stream limit (100): each one's
/// stream credit comes back, so none waits.
#[tokio::test]
async fn abandoned_calls_return_their_streams() {
    let f = start().await;
    let transport = client::connect(&f.url, f.config()).await.unwrap();
    for i in 0..300 {
        let response = tokio::time::timeout(Duration::from_secs(2), transport.send(watch("Watch")))
            .await
            .unwrap_or_else(|_| panic!("call {i} waited for a stream"))
            .unwrap();
        drop(response);
    }
    f.none_held().await;
}

/// What a browser's `reader.cancel()` sends: STOP_SENDING on a stream whose request is
/// complete and whose response is idle. The server has nothing to write that would fail.
#[tokio::test]
async fn stop_sending_from_the_peer_ends_an_idle_call() {
    let f = start().await;
    let connection = Endpoint::client(f.config())
        .unwrap()
        .connect(&f.url)
        .await
        .unwrap();
    let (mut send, mut recv) = connection.open_bi().await.unwrap().await.unwrap();
    send.write_all(b"POST /test.v1.Test/Watch HTTP/1.1\r\ncontent-length: 0\r\n\r\n")
        .await
        .unwrap();
    send.finish().await.unwrap();
    let mut head = [0u8; 12];
    recv.read_exact(&mut head).await.unwrap();
    assert_eq!(&head, b"HTTP/1.1 200");
    assert_eq!(f.held.load(Ordering::SeqCst), 1);
    // Connect `canceled` (1), in HTTP/3's error space.
    recv.stop(VarInt::try_from_u64(0x52e4_a40f_a8db + 1).unwrap());
    f.none_held().await;
}

/// A session that ends takes every call on it with it.
#[tokio::test]
async fn closing_the_session_ends_its_calls() {
    let f = start().await;
    let transport = client::connect(&f.url, f.config()).await.unwrap();
    let mut responses = Vec::new();
    for _ in 0..20 {
        responses.push(transport.send(watch("Watch")).await.unwrap());
    }
    assert_eq!(f.held.load(Ordering::SeqCst), 20);
    transport.close();
    f.none_held().await;
}
