use super::{http3::Masquerade, Hysteria2AuthenticatedInboundProfile};
use std::sync::Arc;

pub(super) fn profile() -> Hysteria2AuthenticatedInboundProfile {
    Hysteria2AuthenticatedInboundProfile {
        protocol: crate::inbound::Hysteria2InboundProfile::from_config("test-password"),
        settings: Default::default(),
        masquerade: Masquerade::NotFound,
    }
}
pub(super) fn endpoint() -> quinn::Endpoint {
    endpoint_with_settings(Default::default())
}
pub(super) fn endpoint_with_settings(settings: crate::settings::Settings) -> quinn::Endpoint {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
    .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(Arc::new(super::congestion::transport(settings).unwrap()));
    quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap()
}
pub(crate) async fn pair() -> (quinn::Connection, quinn::Connection) {
    pair_with_settings(Default::default(), Default::default()).await
}
pub(super) async fn pair_with_settings(
    client_settings: crate::settings::Settings,
    server_settings: crate::settings::Settings,
) -> (quinn::Connection, quinn::Connection) {
    let server = endpoint_with_settings(server_settings);
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    let mut config =
        zero_transport::quic::client_config(true, None, &[b"h3".to_vec()], Some(65536)).unwrap();
    config.transport_config(Arc::new(
        super::congestion::transport(client_settings).unwrap(),
    ));
    client.set_default_client_config(config);
    let connecting = client
        .connect(server.local_addr().unwrap(), "localhost")
        .unwrap();
    let (client, server) = tokio::join!(connecting, async { server.accept().await.unwrap().await });
    (client.unwrap(), server.unwrap())
}
