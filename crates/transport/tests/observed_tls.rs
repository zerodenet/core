#![cfg(feature = "tls")]
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};
use zero_platform_tokio::TokioSocket;
use zero_traits::{IoObserver, TlsBackend};
use zero_transport::{profile::OwnedClientTlsProfile, tls};

#[derive(Debug, Default)]
struct Counts {
    rx: AtomicU64,
    tx: AtomicU64,
}
impl IoObserver for Counts {
    fn received(&self, n: usize) {
        self.rx.fetch_add(n as u64, Ordering::Relaxed);
    }
    fn sent(&self, n: usize) {
        self.tx.fetch_add(n as u64, Ordering::Relaxed);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn tls_socket_observation_includes_handshake_ciphertext_and_survives_raw_handoff() {
    for backend in [TlsBackend::Rustls, TlsBackend::OpenSsl] {
        timeout(Duration::from_secs(5), exchange(backend))
            .await
            .unwrap();
    }
}
async fn exchange(backend: TlsBackend) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
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
    let acceptor = tls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = tls::accept_tls_handshake(&acceptor, socket).await.unwrap();
        let mut request = [0; 7];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        stream.write_all(b"reply").await.unwrap();
        stream.flush().await.unwrap();
        zero_traits::AsyncSocket::transport_bypass_control(&stream)
            .unwrap()
            .request_write_bypass();
        stream.write_all(b"raw").await.unwrap();
        stream.flush().await.unwrap();
    });
    let meter = Arc::new(Counts::default());
    let socket = TokioSocket::new(TcpStream::connect(address).await.unwrap())
        .with_observer(Some(meter.clone()));
    let profile = OwnedClientTlsProfile {
        options: zero_traits::ClientTlsOptions {
            backend,
            ..Default::default()
        },
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: None,
        insecure: true,
        alpn: vec![],
        client_fingerprint: None,
    };
    let mut stream = tls::connect_tls_upstream(socket, &profile, None, "localhost")
        .await
        .unwrap();
    let before_tx = meter.tx.load(Ordering::Relaxed);
    assert!(
        before_tx > 0,
        "physical TLS handshake must be observed for {backend:?}"
    );
    assert!(meter.rx.load(Ordering::Relaxed) > 0);
    stream.write_all(b"request").await.unwrap();
    stream.flush().await.unwrap();
    assert!(meter.tx.load(Ordering::Relaxed) - before_tx > 7);
    let mut reply = [0; 5];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"reply");
    let before_raw_rx = meter.rx.load(Ordering::Relaxed);
    zero_traits::AsyncSocket::transport_bypass_control(&stream)
        .unwrap()
        .request_read_bypass();
    let mut raw = [0; 3];
    stream.read_exact(&mut raw).await.unwrap();
    assert_eq!(&raw, b"raw");
    assert_eq!(meter.rx.load(Ordering::Relaxed) - before_raw_rx, 3);
    server.await.unwrap();
}
