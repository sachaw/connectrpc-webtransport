//! Unary throughput and bidi bandwidth: this client against this server, and against the TypeScript server when Deno is available.

use std::convert::Infallible;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use connectrpc::client::{ClientBody, ClientTransport};
use futures_util::stream;
use http_body::Frame;
use http_body_util::{BodyExt, Full, StreamBody};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tower::service_fn;
use wtransport::{ClientConfig, Endpoint, Identity, ServerConfig};

use connectrpc_webtransport::{Body, client, server};

const RUN: Duration = Duration::from_secs(2);
const UNARY_TEXT: usize = 100;
const FRAME_TEXT: usize = 64 * 1024;

#[tokio::main]
async fn main() {
    let identity = Identity::self_signed_builder()
        .subject_alt_names(["localhost", "127.0.0.1"])
        .from_now_utc()
        .validity_days(14)
        .build()
        .unwrap();
    let leaf = identity.certificate_chain().as_slice()[0].clone();
    let config = || {
        ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([leaf.hash()])
            .build()
    };

    let endpoint = Endpoint::server(
        ServerConfig::builder()
            .with_bind_address("127.0.0.1:0".parse().unwrap())
            .with_identity(identity.clone_identity())
            .build(),
    )
    .unwrap();
    let port = endpoint.local_addr().unwrap().port();
    let cancel = CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move { server::serve(&endpoint, service_fn(echo), Default::default(), cancel).await }
    });
    println!("rust client -> rust server (byte echo, Connect wire)");
    run(&format!("https://127.0.0.1:{port}/rpc"), config).await;

    if let Some((deno_port, _child)) = deno_server(&identity).await {
        println!("\nrust client -> deno server (Connect handlers, protobuf decode/encode)");
        run(&format!("https://127.0.0.1:{deno_port}/rpc"), config).await;
    }
    cancel.cancel();
}

async fn run(url: &str, config: impl Fn() -> ClientConfig) {
    let transport = client::connect(url, config()).await.unwrap();
    for concurrency in [1, 16, 64] {
        let count = Arc::new(AtomicU64::new(0));
        let deadline = Instant::now() + RUN;
        let tasks: Vec<_> = (0..concurrency)
            .map(|_| {
                let (transport, count) = (transport.clone(), count.clone());
                tokio::spawn(async move {
                    let started = Instant::now();
                    while Instant::now() < deadline {
                        let response = match transport
                            .send(request(
                                "/echo.EchoService/Unary",
                                "application/proto",
                                client_body(proto_text(UNARY_TEXT)),
                            ))
                            .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                eprintln!(
                                    "  failed after {} calls, {:?}: {e}",
                                    count.load(Ordering::Relaxed),
                                    started.elapsed()
                                );
                                return;
                            }
                        };
                        response.into_body().collect().await.unwrap();
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        let n = count.load(Ordering::Relaxed);
        println!(
            "  unary x{concurrency:<3} {:>8.0} req/s  {:>7.2} ms/req",
            n as f64 / RUN.as_secs_f64(),
            RUN.as_secs_f64() * 1000.0 * concurrency as f64 / n as f64
        );
    }

    let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(8);
    let body: ClientBody = StreamBody::new(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|f| (f, rx))
    }))
    .map_err(|e: Infallible| match e {})
    .boxed();
    let response = match transport
        .send(request(
            "/echo.EchoService/Bidi",
            "application/connect+proto",
            body,
        ))
        .await
    {
        Ok(response) => response,
        Err(e) => {
            println!("  bidi echo    unavailable: {e}");
            return;
        }
    };
    let frame = Bytes::from(envelope(0, &proto_text(FRAME_TEXT)));
    let deadline = Instant::now() + RUN;
    let producer = tokio::spawn(async move {
        while Instant::now() < deadline {
            if tx.send(Ok(Frame::data(frame.clone()))).await.is_err() {
                break;
            }
        }
    });
    let mut received = 0usize;
    let mut body = response.into_body();
    while let Some(Ok(frame)) = tokio::time::timeout(Duration::from_secs(10), body.frame())
        .await
        .unwrap_or(None)
    {
        received += frame.data_ref().map_or(0, |d| d.len());
    }
    let _ = producer.await;
    println!(
        "  bidi echo {:>8.1} MiB/s (64 KiB messages, echoed bytes counted)",
        received as f64 / RUN.as_secs_f64() / (1024.0 * 1024.0)
    );
}

async fn deno_server(identity: &Identity) -> Option<(u16, tokio::process::Child)> {
    let leaf = &identity.certificate_chain().as_slice()[0];
    let mut child = tokio::process::Command::new("deno")
        .args([
            "run",
            "--allow-net",
            "--allow-env=ECHO_CERT,ECHO_KEY",
            "tests/support/serve.ts",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../ts"))
        .env("ECHO_CERT", leaf.to_pem())
        .env("ECHO_KEY", identity.private_key().to_secret_pem())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .ok()?;
    let mut port = String::new();
    BufReader::new(child.stdout.take()?)
        .read_line(&mut port)
        .await
        .ok()?;
    Some((port.trim().parse().ok()?, child))
}

async fn echo(req: http::Request<Body>) -> Result<http::Response<Body>, Infallible> {
    let content_type = req.headers().get("content-type").cloned();
    let body: Body = if req.uri().path().ends_with("/Bidi") {
        req.into_body()
    } else {
        full(req.into_body().collect().await.unwrap().to_bytes())
    };
    let mut response = http::Response::builder();
    if let Some(ct) = content_type {
        response = response.header("content-type", ct);
    }
    Ok(response.body(body).unwrap())
}

fn request(path: &str, content_type: &str, body: ClientBody) -> http::Request<ClientBody> {
    http::Request::post(format!("https://127.0.0.1{path}"))
        .header("content-type", content_type)
        .body(body)
        .unwrap()
}

fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into())
        .map_err(|e: Infallible| match e {})
        .boxed()
}

fn client_body(bytes: impl Into<Bytes>) -> ClientBody {
    Full::new(bytes.into())
        .map_err(|e: Infallible| match e {})
        .boxed()
}

fn proto_text(len: usize) -> Vec<u8> {
    let mut out = vec![0x0a];
    let mut n = len;
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
    out.resize(out.len() + len, b'x');
    out
}

fn envelope(flags: u8, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + data.len());
    out.push(flags);
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(data);
    out
}
