use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, NamedGroup, RootCertStore};
use zero_transport::fingerprint::{build_client_provider, lookup_fingerprint};

#[test]
fn browser_client_provider_emits_modern_sized_client_hello() {
    let fingerprint = lookup_fingerprint("chrome").expect("chrome fingerprint");
    let provider = build_client_provider(&fingerprint);

    assert_eq!(
        provider.kx_groups[0].name(),
        NamedGroup::X25519MLKEM768,
        "browser-compatible clients must retain the hybrid key share"
    );

    let config = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .expect("supported TLS versions")
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    let server_name = ServerName::try_from("example.com")
        .expect("valid server name")
        .to_owned();
    let mut connection =
        ClientConnection::new(Arc::new(config), server_name).expect("client connection");
    let mut client_hello = Vec::new();
    connection
        .write_tls(&mut client_hello)
        .expect("serialize ClientHello");

    assert!(
        client_hello.len() >= 1_200,
        "modern browser-compatible ClientHello unexpectedly shrank to {} bytes",
        client_hello.len()
    );
}
