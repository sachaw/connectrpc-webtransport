//! A client certificate reaches the service as the connection's peer identity.

#![cfg(all(feature = "server", feature = "client"))]

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use connectrpc::client::{ClientBody, ClientTransport};
use http_body_util::{BodyExt, Full};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use tokio_util::sync::CancellationToken;
use tower::service_fn;
use wtransport::tls::rustls;
use wtransport::tls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use wtransport::tls::rustls::server::WebPkiClientVerifier;
use wtransport::{ClientConfig, Connection, Endpoint, ServerConfig};

use connectrpc_webtransport::{Body, client, server};

fn leaf(
    ca: &CertifiedIssuer<'_, KeyPair>,
    names: &[&str],
    purpose: ExtendedKeyUsagePurpose,
) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let key = KeyPair::generate().unwrap();
    let mut params =
        CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>()).unwrap();
    params.extended_key_usages = vec![purpose];
    let cert = params.signed_by(&key, &**ca).unwrap();
    (
        cert.der().clone(),
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
    )
}

async fn whoami(req: http::Request<Body>) -> Result<http::Response<Body>, Infallible> {
    let peer = req
        .extensions()
        .get::<Connection>()
        .and_then(|c| c.peer_identity())
        .map(|chain| chain.as_slice()[0].der().len().to_string());
    let mut response = http::Response::builder();
    if let Some(peer) = peer {
        response = response.header("x-peer-cert-len", peer);
    }
    let body = Full::new(Bytes::new())
        .map_err(|e: Infallible| match e {})
        .boxed();
    Ok(response.body(body).unwrap())
}

#[tokio::test]
async fn client_certificate_is_the_peer_identity() {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca.der().clone()).unwrap();
    let roots = Arc::new(roots);

    let (server_cert, server_key) = leaf(&ca, &["127.0.0.1"], ExtendedKeyUsagePurpose::ServerAuth);
    let mut server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_client_cert_verifier(
            WebPkiClientVerifier::builder(roots.clone())
                .build()
                .unwrap(),
        )
        .with_single_cert(vec![server_cert], server_key)
        .unwrap();
    server_tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
    let endpoint = Endpoint::server(
        ServerConfig::builder()
            .with_bind_address("127.0.0.1:0".parse().unwrap())
            .with_custom_tls(server_tls)
            .build(),
    )
    .unwrap();
    let port = endpoint.local_addr().unwrap().port();
    let cancel = CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move { server::serve(&endpoint, service_fn(whoami), Default::default(), cancel).await }
    });

    let (client_cert, client_key) = leaf(&ca, &[], ExtendedKeyUsagePurpose::ClientAuth);
    let client_cert_len = client_cert.len();
    let mut client_tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![client_cert], client_key)
        .unwrap();
    client_tls.alpn_protocols = vec![wtransport::tls::WEBTRANSPORT_ALPN.to_vec()];
    let transport = client::connect(
        &format!("https://127.0.0.1:{port}/rpc"),
        ClientConfig::builder()
            .with_bind_default()
            .with_custom_tls(client_tls)
            .build(),
    )
    .await
    .unwrap();

    let body: ClientBody = Full::new(Bytes::new())
        .map_err(|e: Infallible| match e {})
        .boxed();
    let response = transport
        .send(
            http::Request::post("https://127.0.0.1/whoami")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers()["x-peer-cert-len"],
        client_cert_len.to_string()
    );
    cancel.cancel();
}
