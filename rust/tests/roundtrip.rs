#![cfg(all(feature = "server", feature = "client"))]

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use connectrpc::client::{ClientBody, ClientTransport};
use http_body_util::{BodyExt, Full, StreamBody};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use tower::service_fn;
use wtransport::endpoint::endpoint_side::Server;
use wtransport::tls::Sha256Digest;
use wtransport::{ClientConfig, Endpoint, Identity, ServerConfig};

use connectrpc_webtransport::{Body, client, server};

async fn echo(req: http::Request<Body>) -> Result<http::Response<Body>, Infallible> {
    let status = if req.uri().path().ends_with("/Missing") {
        404
    } else {
        200
    };
    let path = req.uri().path().to_string();
    let method = req.method().to_string();
    let host = req.headers().get("host").cloned();
    let peer = req
        .extensions()
        .get::<wtransport::Connection>()
        .map(|c| c.remote_address().to_string());
    let body: Body = if path.ends_with("/Bidi") {
        // Echo frames as they arrive, so the response streams alongside the request.
        req.into_body()
    } else {
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        Full::new(bytes).map_err(|e: Infallible| match e {}).boxed()
    };
    let mut response = http::Response::builder()
        .status(status)
        .header("x-req-method", method)
        .header("x-req-path", path);
    if let Some(host) = host {
        response = response.header("x-req-host", host);
    }
    if let Some(peer) = peer {
        response = response.header("x-req-peer", peer);
    }
    Ok(response.body(body).unwrap())
}

struct Fixture {
    endpoint: Arc<Endpoint<Server>>,
    digest: Sha256Digest,
    cancel: CancellationToken,
    serving: Option<tokio::task::JoinHandle<()>>,
    /// The server-side connection of the most recent call.
    peer: Arc<std::sync::Mutex<Option<wtransport::Connection>>>,
}

const DRAIN: Duration = Duration::from_millis(300);

impl Drop for Fixture {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Fixture {
    fn url(&self) -> String {
        format!(
            "https://127.0.0.1:{}/rpc",
            self.endpoint.local_addr().unwrap().port()
        )
    }

    fn serve(&mut self) {
        let (endpoint, cancel, peer) = (
            self.endpoint.clone(),
            self.cancel.clone(),
            self.peer.clone(),
        );
        let options = server::ServeOptions {
            drain: DRAIN,
            ..Default::default()
        };
        let service = service_fn(move |req: http::Request<Body>| {
            *peer.lock().unwrap() = req.extensions().get::<wtransport::Connection>().cloned();
            echo(req)
        });
        self.serving = Some(tokio::spawn(async move {
            server::serve(&endpoint, service, options, cancel).await
        }));
    }

    /// Drop every session and start serving again on the same endpoint.
    fn restart(&mut self) {
        self.cancel.cancel();
        self.cancel = CancellationToken::new();
        self.serve();
    }

    fn client_config(&self) -> ClientConfig {
        ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([self.digest.clone()])
            .build()
    }
}

async fn start() -> Fixture {
    let identity = Identity::self_signed_builder()
        .subject_alt_names(["localhost", "127.0.0.1"])
        .from_now_utc()
        .validity_days(14)
        .build()
        .unwrap();
    let digest = identity.certificate_chain().as_slice()[0].hash();
    let endpoint = Arc::new(
        Endpoint::server(
            ServerConfig::builder()
                .with_bind_address("127.0.0.1:0".parse().unwrap())
                .with_identity(identity)
                .build(),
        )
        .unwrap(),
    );
    let mut f = Fixture {
        endpoint,
        digest,
        cancel: CancellationToken::new(),
        serving: None,
        peer: Default::default(),
    };
    f.serve();
    f
}

fn request(path: &str, body: ClientBody) -> http::Request<ClientBody> {
    http::Request::post(format!("https://127.0.0.1:1{path}"))
        .header("content-type", "application/proto")
        .body(body)
        .unwrap()
}

fn full(bytes: &'static [u8]) -> ClientBody {
    Full::new(Bytes::from_static(bytes))
        .map_err(|e: Infallible| match e {})
        .boxed()
}

#[tokio::test]
async fn unary_round_trips_with_status_and_headers() {
    let f = start().await;
    let transport = client::connect(&f.url(), f.client_config()).await.unwrap();

    let response = transport
        .send(request("/test.v1.Test/Ping", full(b"ping")))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["x-req-method"], "POST");
    assert_eq!(response.headers()["x-req-path"], "/test.v1.Test/Ping");
    assert_eq!(response.headers()["x-req-host"], "127.0.0.1:1");
    assert!(
        response.headers()["x-req-peer"]
            .to_str()
            .unwrap()
            .starts_with("127.0.0.1:")
    );
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        &b"ping"[..]
    );

    let response = transport
        .send(request("/test.v1.Test/Missing", full(b"")))
        .await
        .unwrap();
    assert_eq!(response.status(), 404);

    let get = http::Request::get("/test.v1.Test/Ping?message=e30")
        .body(full(b""))
        .unwrap();
    let response = transport.send(get).await.unwrap();
    assert_eq!(response.headers()["x-req-method"], "GET");
    assert_eq!(response.headers()["x-req-path"], "/test.v1.Test/Ping");
}

#[tokio::test]
async fn streaming_body_is_sent_chunked() {
    let f = start().await;
    let transport = client::connect(&f.url(), f.client_config()).await.unwrap();

    let frames = futures_util::stream::iter(
        [&b"a"[..], b"b", b"c"].map(|c| Ok(http_body::Frame::data(Bytes::from_static(c)))),
    );
    let body: ClientBody = StreamBody::new(frames).boxed();
    assert_eq!(http_body::Body::size_hint(&body).exact(), None);

    let response = transport
        .send(request("/test.v1.Test/Stream", body))
        .await
        .unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        &b"abc"[..]
    );
}

/// A raw stream; the connection is returned so it outlives the streams.
async fn raw_stream(
    f: &Fixture,
) -> (
    wtransport::Connection,
    wtransport::SendStream,
    wtransport::RecvStream,
) {
    let connection = Endpoint::client(f.client_config())
        .unwrap()
        .connect(f.url())
        .await
        .unwrap();
    let (send, recv) = connection.open_bi().await.unwrap().await.unwrap();
    (connection, send, recv)
}

async fn read_status(recv: &mut wtransport::RecvStream) -> String {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n") {
        tokio::time::timeout(Duration::from_secs(5), recv.read_exact(&mut byte))
            .await
            .expect("response must arrive although the request stream is still open")
            .unwrap();
        buf.push(byte[0]);
    }
    String::from_utf8(buf).unwrap()
}

#[tokio::test]
async fn content_length_dispatches_without_fin() {
    let f = start().await;
    let (_connection, mut send, mut recv) = raw_stream(&f).await;
    send.write_all(b"POST /test.v1.Test/Ping HTTP/1.1\r\ncontent-length: 4\r\n\r\nping")
        .await
        .unwrap();
    assert_eq!(read_status(&mut recv).await, "HTTP/1.1 200 OK\r\n");
}

#[tokio::test]
async fn chunked_request_dispatches_on_its_terminal_chunk_without_fin() {
    let f = start().await;
    let (_connection, mut send, mut recv) = raw_stream(&f).await;
    send.write_all(
        b"POST /test.v1.Test/Ping HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n\
          2\r\npi\r\n2\r\nng\r\n0\r\n\r\n",
    )
    .await
    .unwrap();
    assert_eq!(read_status(&mut recv).await, "HTTP/1.1 200 OK\r\n");
    let mut rest = Vec::new();
    recv.read_to_end(&mut rest).await.unwrap();
    assert!(
        rest.ends_with(b"\r\n\r\nping"),
        "{}",
        String::from_utf8_lossy(&rest)
    );
}

#[tokio::test]
async fn bidi_is_full_duplex() {
    let f = start().await;
    let transport = client::connect(&f.url(), f.client_config()).await.unwrap();

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<http_body::Frame<Bytes>, Infallible>>(4);
    let body: ClientBody = StreamBody::new(tokio_stream_from(rx))
        .map_err(|e: Infallible| match e {})
        .boxed();
    tx.send(Ok(http_body::Frame::data(Bytes::from_static(b"first"))))
        .await
        .unwrap();

    let mut response = tokio::time::timeout(
        Duration::from_secs(3),
        transport.send(request("/test.v1.Test/Bidi", body)),
    )
    .await
    .expect("response head must arrive while the request body is still open")
    .unwrap();
    let echoed = tokio::time::timeout(Duration::from_secs(3), response.body_mut().frame())
        .await
        .expect("first frame must be echoed while the request body is still open")
        .unwrap()
        .unwrap();
    assert_eq!(echoed.into_data().unwrap(), &b"first"[..]);

    tx.send(Ok(http_body::Frame::data(Bytes::from_static(b"second"))))
        .await
        .unwrap();
    drop(tx);
    let rest = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&rest[..], b"second");
}

fn tokio_stream_from<T>(
    mut rx: tokio::sync::mpsc::Receiver<T>,
) -> impl futures_util::Stream<Item = T> {
    futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx))
}

#[tokio::test]
async fn reconnecting_transport_opens_lazily_shares_one_dial_and_backs_off() {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use connectrpc_webtransport::client::{LinkPhase, LinkState, ReconnectOptions, Transport};

    let mut f = start().await;
    let endpoint = Arc::new(wtransport::Endpoint::client(f.client_config()).unwrap());
    let url = f.url();
    let opens = Arc::new(AtomicU32::new(0));
    let states = Arc::new(Mutex::new(Vec::<LinkState>::new()));

    let transport = Transport::reconnecting(
        {
            let (opens, endpoint) = (opens.clone(), endpoint.clone());
            move || {
                let n = opens.fetch_add(1, Ordering::SeqCst) + 1;
                let (endpoint, url) = (endpoint.clone(), url.clone());
                async move {
                    if n < 3 {
                        return Err(connectrpc_webtransport::Error::Endpoint(
                            std::io::Error::other("refused"),
                        ));
                    }
                    Ok(endpoint.connect(url).await?)
                }
            }
        },
        ReconnectOptions {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            on_state: Some(Box::new({
                let states = states.clone();
                move |s| states.lock().unwrap().push(s)
            })),
        },
    );
    assert_eq!(opens.load(Ordering::SeqCst), 0);

    let (a, b) = tokio::join!(
        transport.send(request("/test.v1.Test/Ping", full(b"1"))),
        transport.send(request("/test.v1.Test/Ping", full(b"2"))),
    );
    assert!(a.is_err() && b.is_err());
    assert_eq!(
        opens.load(Ordering::SeqCst),
        1,
        "concurrent callers share one attempt"
    );

    assert!(
        transport
            .send(request("/test.v1.Test/Ping", full(b"3")))
            .await
            .is_err()
    );
    let response = transport
        .send(request("/test.v1.Test/Ping", full(b"4")))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(opens.load(Ordering::SeqCst), 3);
    assert_eq!(
        states
            .lock()
            .unwrap()
            .iter()
            .map(|s| (s.phase, s.failures))
            .collect::<Vec<_>>(),
        [
            (LinkPhase::Connecting, 1),
            (LinkPhase::Connecting, 2),
            (LinkPhase::Connected, 0)
        ]
    );

    f.restart();
    tokio::time::timeout(Duration::from_secs(5), async {
        while states.lock().unwrap().len() < 4 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the closed session must be announced");
    assert_eq!(states.lock().unwrap()[3].phase, LinkPhase::Reconnecting);

    let response = transport
        .send(request("/test.v1.Test/Ping", full(b"5")))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(opens.load(Ordering::SeqCst), 4);
    assert_eq!(states.lock().unwrap()[4].phase, LinkPhase::Connected);
}

#[tokio::test]
async fn connection_outlives_its_endpoint_handle() {
    let f = start().await;
    let connection = Endpoint::client(f.client_config())
        .unwrap()
        .connect(f.url())
        .await
        .unwrap();
    let transport = client::Transport::new(connection);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let response = transport
        .send(request("/test.v1.Test/Ping", full(b"ping")))
        .await
        .unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        &b"ping"[..]
    );
}

#[tokio::test]
async fn shutdown_drains_in_flight_calls_then_closes() {
    let mut f = start().await;
    let transport = client::connect(&f.url(), f.client_config()).await.unwrap();

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<http_body::Frame<Bytes>, Infallible>>(4);
    let body: ClientBody = StreamBody::new(tokio_stream_from(rx))
        .map_err(|e: Infallible| match e {})
        .boxed();
    tx.send(Ok(http_body::Frame::data(Bytes::from_static(b"open"))))
        .await
        .unwrap();
    let mut response = transport
        .send(request("/test.v1.Test/Bidi", body))
        .await
        .unwrap();
    response.body_mut().frame().await.unwrap().unwrap();

    let started = std::time::Instant::now();
    f.cancel.cancel();
    tx.send(Ok(http_body::Frame::data(Bytes::from_static(b"more"))))
        .await
        .unwrap();
    let echoed = response.body_mut().frame().await.unwrap().unwrap();
    assert_eq!(echoed.into_data().unwrap(), &b"more"[..]);

    assert!(response.into_body().collect().await.is_err());
    tokio::time::timeout(Duration::from_secs(5), f.serving.take().unwrap())
        .await
        .expect("serve must return after the drain")
        .unwrap();
    let elapsed = started.elapsed();
    assert!(elapsed >= DRAIN && elapsed < DRAIN * 5, "{elapsed:?}");
    drop(tx);
}

#[tokio::test]
async fn close_ends_the_session_and_stops_redialling() {
    use std::sync::atomic::{AtomicU32, Ordering};

    use connectrpc_webtransport::client::{ReconnectOptions, Transport};

    let f = start().await;
    let endpoint = Arc::new(wtransport::Endpoint::client(f.client_config()).unwrap());
    let url = f.url();
    let opens = Arc::new(AtomicU32::new(0));
    let transport = Transport::reconnecting(
        {
            let (opens, endpoint) = (opens.clone(), endpoint.clone());
            move || {
                opens.fetch_add(1, Ordering::SeqCst);
                let (endpoint, url) = (endpoint.clone(), url.clone());
                async move { Ok(endpoint.connect(url).await?) }
            }
        },
        ReconnectOptions::default(),
    );
    transport
        .send(request("/test.v1.Test/Ping", full(b"1")))
        .await
        .unwrap();

    transport.close();
    assert!(
        transport
            .send(request("/test.v1.Test/Ping", full(b"2")))
            .await
            .is_err()
    );
    assert_eq!(opens.load(Ordering::SeqCst), 1);

    let fixed = client::connect(&f.url(), f.client_config()).await.unwrap();
    fixed.close();
    assert!(
        fixed
            .send(request("/test.v1.Test/Ping", full(b"3")))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn dropping_the_last_handle_ends_a_reconnecting_session() {
    let f = start().await;
    let transport = client::reconnect(f.url(), f.client_config(), Default::default()).unwrap();
    transport
        .send(request("/test.v1.Test/Ping", full(b"1")))
        .await
        .unwrap();
    let connection = f.peer.lock().unwrap().clone().unwrap();
    drop(transport);
    tokio::time::timeout(Duration::from_secs(2), connection.closed())
        .await
        .expect("the session must close once no handle remains");
}
