//! Runs the TypeScript transport (Deno) against this server over real QUIC.
//! Skipped when `deno` is not installed.

#![cfg(feature = "server")]

use std::convert::Infallible;
use std::process::Stdio;

use bytes::{Buf, Bytes, BytesMut};
use futures_util::stream;
use http::header::CONTENT_TYPE;
use http::{Request, Response};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tower::service_fn;
use wtransport::tls::Sha256DigestFmt;
use wtransport::{Endpoint, Identity, ServerConfig};

use connectrpc_webtransport::Body;

const END_STREAM: u8 = 0b10;

#[tokio::test]
async fn typescript_transport_conforms() {
    if tokio::process::Command::new("deno")
        .arg("--version")
        .stdout(Stdio::null())
        .status()
        .await
        .is_err()
    {
        eprintln!("skipping: deno not found");
        return;
    }

    let identity = Identity::self_signed_builder()
        .subject_alt_names(["localhost", "127.0.0.1"])
        .from_now_utc()
        .validity_days(14)
        .build()
        .unwrap();
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
