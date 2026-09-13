use std::{
    io::{Read, Write},
    sync::Arc,
};
use ztls::{
    fingerprint::ClientHelloProfile as P,
    handshake::{Tls13Config, Tls13Connection},
};
fn exchange(profile: P, group: u16) {
    let cert = rcgen::generate_simple_self_signed(vec!["example.com".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    provider.kx_groups.retain(|g| u16::from(g.name()) == group);
    let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key.into())
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
    )
    .build()
    .unwrap();
    let mut client = Tls13Connection::new(Tls13Config {
        server_name: "example.com".into(),
        client_hello_profile: profile,
        server_verifier: Some(verifier),
        ..Default::default()
    })
    .unwrap();
    let mut server = rustls::ServerConnection::new(Arc::new(server_config)).unwrap();
    for round in 0..10 {
        let mut wire = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut wire).unwrap();
        }
        if round == 0 {
            assert_eq!(&wire[..3], &[22, 3, 1]);
        }
        if !wire.is_empty() {
            server.read_tls(&mut &wire[..]).unwrap();
            server.process_new_packets().unwrap();
        }
        wire.clear();
        while server.wants_write() {
            server.write_tls(&mut wire).unwrap();
        }
        if !wire.is_empty() {
            client.read_tls(&mut &wire[..]).unwrap();
            client.process_new_packets().unwrap();
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            break;
        }
    }
    assert!(
        !client.is_handshaking() && !server.is_handshaking(),
        "{profile} group {group}"
    );
    client.write_plaintext(b"client payload");
    let mut wire = Vec::new();
    client.write_tls(&mut wire).unwrap();
    server.read_tls(&mut &wire[..]).unwrap();
    server.process_new_packets().unwrap();
    let mut payload = [0; 14];
    server.reader().read_exact(&mut payload).unwrap();
    assert_eq!(&payload, b"client payload");
    server.writer().write_all(b"server payload").unwrap();
    wire.clear();
    server.write_tls(&mut wire).unwrap();
    client.read_tls(&mut &wire[..]).unwrap();
    client.process_new_packets().unwrap();
    assert_eq!(client.read_plaintext(&mut payload), 14);
    assert_eq!(&payload, b"server payload");
}
#[test]
fn all_versioned_profiles_complete_verified_tls_and_bidirectional_records() {
    for &p in P::VERSIONED {
        exchange(p, 29);
    }
    for p in [P::Random, P::Randomized, P::RandomizedNoAlpn] {
        exchange(p, 29);
    }
}
#[test]
fn secondary_and_hybrid_key_shares_are_negotiable() {
    for p in [P::Firefox99, P::Firefox120, P::Firefox148] {
        exchange(p, 23);
    }
    for p in [P::Chrome131, P::Chrome133, P::Firefox148, P::Safari263] {
        exchange(p, 4588);
    }
}

#[test]
fn hello_retry_request_replaces_the_key_share_and_preserves_the_transcript() {
    // Chrome 83 advertises P-256/P-384 but initially sends only X25519.
    exchange(P::Chrome83, 23);
    exchange(P::Chrome83, 24);
}
