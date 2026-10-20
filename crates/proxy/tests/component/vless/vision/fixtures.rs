use std::sync::Arc;
pub(super) fn uuid() -> [u8; 16] {
    super::super::fixtures::request()[1..17].try_into().unwrap()
}
pub(super) fn request() -> Vec<u8> {
    let plain = super::super::fixtures::request();
    let mut bytes = plain[..17].to_vec();
    bytes.extend_from_slice(&[18, 0x0a, 16]);
    bytes.extend_from_slice(b"xtls-rprx-vision");
    bytes.extend_from_slice(&plain[18..]);
    bytes
}
pub(super) fn frame(first: bool, command: u8, content: &[u8], padding: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    if first {
        bytes.extend_from_slice(&uuid());
    }
    bytes.push(command);
    bytes.extend_from_slice(&(content.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&(padding as u16).to_be_bytes());
    bytes.extend_from_slice(content);
    bytes.resize(bytes.len() + padding, 0);
    bytes
}
pub(super) fn inner_tls() -> (tokio_rustls::TlsAcceptor, Arc<rustls::ClientConfig>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        cert.signing_key.serialize_der(),
    ));
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key)
        .unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (
        tokio_rustls::TlsAcceptor::from(Arc::new(server)),
        Arc::new(client),
    )
}
pub(super) fn tls12_acceptor() -> tokio_rustls::TlsAcceptor {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        cert.signing_key.serialize_der(),
    ));
    let server = rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key)
        .unwrap();
    tokio_rustls::TlsAcceptor::from(Arc::new(server))
}
