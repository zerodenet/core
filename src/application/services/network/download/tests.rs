use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn real_https_validation_failure_retires_before_a_trusted_tls_exchange() {
    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let certificate = cert.cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate.clone()], key.into())
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        assert!(
            acceptor.accept(first).await.is_err(),
            "default roots must reject this actual self-signed peer"
        );
        let (next, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(next).await.unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            header.push(tls.read_u8().await.unwrap());
        }
        tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nsecure")
            .await
            .unwrap();
        tls.shutdown().await.unwrap();
    });
    let network = ApplicationEventSinkTcpDialer::new(EgressInterfaceControl::default());
    network.begin_generation().unwrap();
    let first_epoch = network.current().unwrap();
    let mut rejected = network
        .download(format!("https://{address}/rules").parse().unwrap())
        .unwrap();
    assert!(rejected.next().await.is_err());
    drop(rejected);
    assert!(!first_epoch.closed(), "TLS rejection is not original close");
    network
        .retire_generation(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(first_epoch.closed());
    // The test trusts its actual generated certificate; production continues
    // to use the unchanged verified public root store, with no insecure path.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let trusted = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    network.begin_generation().unwrap();
    let mut accepted = network
        .download_using(
            format!("https://{address}/rules").parse().unwrap(),
            Some(Arc::new(trusted)),
        )
        .unwrap();
    let mut bytes = Vec::new();
    while let Some(event) = accepted.next().await.unwrap() {
        match event {
            DownloadEvent::Data(data) => bytes.extend_from_slice(&data),
            DownloadEvent::Redirect(_) => panic!("real TLS peer did not redirect"),
        }
    }
    assert_eq!(bytes, b"secure");
    drop(accepted);
    network
        .retire_generation(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(network.current().unwrap().closed());
    peer.await.unwrap();
}
