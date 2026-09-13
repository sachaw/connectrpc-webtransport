//! Runs the TypeScript client against this server and this client against the TypeScript server, over real QUIC.
//! Skipped when `deno` is not installed.

#![cfg(all(feature = "server", feature = "client"))]

use std::convert::Infallible;
use std::process::Stdio;
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use connectrpc::client::{ClientBody, ClientTransport};
use futures_util::stream;
use http::header::CONTENT_TYPE;
use http::{Request, Response};
use http_body::Frame;
use http_body_util::{BodyExt, Full, StreamBody};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tower::service_fn;
use wtransport::tls::Sha256DigestFmt;
use wtransport::{ClientConfig, Endpoint, Identity, ServerConfig};

use connectrpc_webtransport::{Body, client};

const END_STREAM: u8 = 0b10;
const TS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../ts");

async fn deno_available() -> bool {
    tokio::process::Command::new("deno")
        .arg("--version")
        .stdout(Stdio::null())
        .status()
        .await
        .is_ok()
}

fn identity() -> Identity {
    Identity::self_signed_builder()
        .subject_alt_names(["localhost", "127.0.0.1"])
        .from_now_utc()
        .validity_days(14)
        .build()
        .unwrap()
}

#[tokio::test]
async fn typescript_client_conforms() {
    if !deno_available().await {
        eprintln!("skipping: deno not found");
        return;
    }

    let identity = identity();
    let hash = identity.certificate_chain().as_slice()[0]
        .hash()
        .fmt(Sha256DigestFmt::DottedHex);
    let endpoint = Endpoint::server(
        ServerConfig::builder()
            .with_bind_address("127.0.0.1:0".parse().unwrap())
            .with_identity(identity)
            .build(),
    )
    .unwrap();
    let port = endpoint.local_addr().unwrap().port();
    let cancel = CancellationToken::new();
    let server = tokio::spawn({
        let cancel = cancel.clone();
        async move { connectrpc_webtransport::server::serve(&endpoint, service_fn(echo), cancel).await }
    });

    let status = tokio::process::Command::new("deno")
        .args([
            "test",
            "--allow-net",
            "--allow-env=ECHO_PORT,ECHO_HASH",
            "src/conformance_test.ts",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../ts"))
        .env("ECHO_PORT", port.to_string())
        .env("ECHO_HASH", hash)
        .status()
        .await
        .unwrap();
    cancel.cancel();
    server.await.unwrap();
    assert!(status.success(), "deno test failed: {status}");
}

async fn echo(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let content_type = req.headers().get(CONTENT_TYPE).cloned();
    let (status, body): (u16, Body) = match req.uri().path().rsplit('/').next() {
        Some("Unary") => (200, req.into_body()),
        Some("Fail") => (
            404,
            full(r#"{"code":"not_found","message":"no such thing"}"#),
        ),
        Some("ServerStream") => {
            let message = envelopes(req.into_body().collect().await.unwrap().to_bytes())
                .into_iter()
                .next()
                .unwrap_or_default();
            let mut out = BytesMut::new();
            for _ in 0..3 {
                out.extend_from_slice(&envelope(0, &message));
            }
            out.extend_from_slice(&envelope(
                END_STREAM,
                br#"{"metadata":{"x-echo-trailer":["yes"]}}"#,
            ));
            (200, full(out.freeze()))
        }
        Some("Bidi") => (200, bidi(req.into_body())),
        _ => (
            404,
            full(r#"{"code":"unimplemented","message":"unknown method"}"#),
        ),
    };
    let mut response = Response::builder().status(status);
    if let Some(ct) = content_type.filter(|_| status == 200) {
        response = response.header(CONTENT_TYPE, ct);
    } else {
        response = response.header(CONTENT_TYPE, "application/json");
    }
    Ok(response.body(body).unwrap())
}

fn bidi(mut input: Body) -> Body {
    let (tx, rx) = mpsc::channel::<Bytes>(16);
    tokio::spawn(async move {
        let mut buf = BytesMut::new();
        while let Some(Ok(frame)) = input.frame().await {
            if let Ok(data) = frame.into_data() {
                buf.extend_from_slice(&data);
            }
            for message in drain_envelopes(&mut buf) {
                if tx.send(envelope(0, &message)).await.is_err() {
                    return;
                }
            }
        }
        let _ = tx.send(envelope(END_STREAM, b"{}")).await;
    });
    StreamBody::new(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|b| (Ok(Frame::data(b)), rx))
    }))
    .boxed()
}

fn full(bytes: impl Into<Bytes>) -> Body {
    http_body_util::Full::new(bytes.into())
        .map_err(|e: Infallible| match e {})
        .boxed()
}

fn envelope(flags: u8, data: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(5 + data.len());
    out.extend_from_slice(&[flags]);
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(data);
    out.freeze()
}

fn envelopes(bytes: Bytes) -> Vec<Bytes> {
    let mut buf = BytesMut::from(bytes);
    drain_envelopes(&mut buf)
}

fn drain_envelopes(buf: &mut BytesMut) -> Vec<Bytes> {
    let mut out = Vec::new();
    while buf.len() >= 5 {
        let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
        if buf.len() < 5 + len {
            break;
        }
        buf.advance(5);
        out.push(buf.split_to(len).freeze());
    }
    out
}

fn proto_text(text: &str) -> Vec<u8> {
    let mut out = vec![0x0a, text.len() as u8];
    out.extend_from_slice(text.as_bytes());
    out
}

fn request(path: &str, content_type: &str, body: ClientBody) -> http::Request<ClientBody> {
    http::Request::post(format!("https://127.0.0.1{path}"))
        .header("content-type", content_type)
        .body(body)
        .unwrap()
}

fn client_body(bytes: impl Into<Bytes>) -> ClientBody {
    Full::new(bytes.into())
        .map_err(|e: Infallible| match e {})
        .boxed()
}

#[tokio::test]
async fn typescript_server_conforms() {
    if !deno_available().await {
        eprintln!("skipping: deno not found");
        return;
    }

    let identity = identity();
    let leaf = &identity.certificate_chain().as_slice()[0];
    let mut server = tokio::process::Command::new("deno")
        .args([
            "run",
            "--allow-net",
            "--allow-env=ECHO_CERT,ECHO_KEY",
            "testing/serve.ts",
        ])
        .current_dir(TS_DIR)
        .env("ECHO_CERT", leaf.to_pem())
        .env("ECHO_KEY", identity.private_key().to_secret_pem())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut port = String::new();
    BufReader::new(server.stdout.take().unwrap())
        .read_line(&mut port)
        .await
        .unwrap();
    let port: u16 = port
        .trim()
        .parse()
        .expect("deno server must print its port");

    let transport = client::connect(
        &format!("https://127.0.0.1:{port}/echo"),
        ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([leaf.hash()])
            .build(),
    )
    .await
    .unwrap();

    let response = transport
        .send(request(
            "/echo.EchoService/Unary",
            "application/proto",
            client_body(proto_text("hello")),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        proto_text("hello")
    );

    let response = transport
        .send(request(
            "/echo.EchoService/Fail",
            "application/proto",
            client_body(proto_text("x")),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        body.starts_with(br#"{"code":"not_found""#),
        "{}",
        String::from_utf8_lossy(&body)
    );

    let message = proto_text("s");
    let response = transport
        .send(request(
            "/echo.EchoService/ServerStream",
            "application/connect+proto",
            client_body(envelope(0, &message)),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut body = BytesMut::from(response.into_body().collect().await.unwrap().to_bytes());
    let frames = drain_frames(&mut body);
    assert_eq!(frames.len(), 4);
    assert!(frames[..3].iter().all(|(f, m)| *f == 0 && m == &message));
    assert_eq!(frames[3].0, END_STREAM);
    assert!(
        std::str::from_utf8(&frames[3].1)
            .unwrap()
            .contains("x-echo-trailer")
    );

    let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(4);
    let body: ClientBody = StreamBody::new(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|f| (f, rx))
    }))
    .map_err(|e: Infallible| match e {})
    .boxed();
    tx.send(Ok(Frame::data(envelope(0, &proto_text("a")))))
        .await
        .unwrap();
    let mut response = transport
        .send(request(
            "/echo.EchoService/Bidi",
            "application/connect+proto",
            body,
        ))
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(3), response.body_mut().frame())
        .await
        .expect("echo must arrive while the request is still open")
        .unwrap()
        .unwrap();
    assert_eq!(first.into_data().unwrap(), envelope(0, &proto_text("a")));
    tx.send(Ok(Frame::data(envelope(0, &proto_text("b")))))
        .await
        .unwrap();
    drop(tx);
    let mut rest = BytesMut::from(response.into_body().collect().await.unwrap().to_bytes());
    let frames = drain_frames(&mut rest);
    assert_eq!(frames[0], (0, Bytes::from(proto_text("b"))));
    assert_eq!(frames.last().unwrap().0, END_STREAM);
}

fn drain_frames(buf: &mut BytesMut) -> Vec<(u8, Bytes)> {
    let mut out = Vec::new();
    while buf.len() >= 5 {
        let flags = buf[0];
        let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
        if buf.len() < 5 + len {
            break;
        }
        buf.advance(5);
        out.push((flags, buf.split_to(len).freeze()));
    }
    out
}
