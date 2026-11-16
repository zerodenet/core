use super::*;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::sync::{Arc, Mutex};
#[derive(Debug)]
struct Observe {
    real: Arc<dyn ServerCertVerifier>,
    staple: Arc<Mutex<Vec<u8>>>,
}
impl ServerCertVerifier for Observe {
    fn verify_server_cert(
        &self,
        leaf: &CertificateDer<'_>,
        chain: &[CertificateDer<'_>],
        name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let result = self.real.verify_server_cert(leaf, chain, name, ocsp, now)?;
        *self.staple.lock().unwrap() = ocsp.to_vec();
        Ok(result)
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.real.verify_tls12_signature(message, cert, signature)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.real.verify_tls13_signature(message, cert, signature)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.real.supported_verify_schemes()
    }
}
pub(super) fn client(
    roots: Vec<CertificateDer<'static>>,
) -> (rustls::ClientConfig, Arc<Mutex<Vec<u8>>>) {
    let mut trust = rustls::RootCertStore::empty();
    for root in roots {
        trust.add(root).unwrap();
    }
    let trust = Arc::new(trust);
    let real = rustls::client::WebPkiServerVerifier::builder_with_provider(
        trust.clone(),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .unwrap();
    let staple = Arc::new(Mutex::new(Vec::new()));
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(trust)
    .with_no_client_auth();
    config
        .dangerous()
        .set_certificate_verifier(Arc::new(Observe {
            real,
            staple: staple.clone(),
        }));
    (config, staple)
}
pub(super) async fn peek(
    endpoint: std::net::SocketAddr,
    config: &rustls::ClientConfig,
    name: &str,
) -> Vec<u8> {
    let raw = tokio::net::TcpStream::connect(endpoint).await.unwrap();
    let mut io = tokio_rustls::TlsConnector::from(Arc::new(config.clone()))
        .connect(ServerName::try_from(name.to_owned()).unwrap(), raw)
        .await
        .unwrap();
    let leaf = io.get_ref().1.peer_certificates().unwrap()[0].to_vec();
    io.shutdown().await.unwrap();
    leaf
}
pub(super) async fn start(
    config: serde_json::Value,
) -> (crate::Proxy, crate::RunningProxy, std::net::SocketAddr) {
    let config = zero_config::RuntimeConfig::parse(&config.to_string()).unwrap();
    let proxy = crate::Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    )
    .apply_runtime_config_and_wait(config, Duration::from_secs(10))
    .await
    .unwrap();
    let owner = super::super::inbound_receipts::owner(&proxy, "entry").await;
    let endpoint = *owner.original_listener_address.get().unwrap();
    (proxy, running, endpoint)
}
pub(super) async fn exercise(socks: std::net::SocketAddr) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let target = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4];
        io.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"wire");
        io.write_all(&bytes).await.unwrap();
        io.shutdown().await.unwrap();
    });
    let mut io = tokio::net::TcpStream::connect(socks).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut reply = [0; 2];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend(port.to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..2], &[5, 0]);
    io.write_all(b"wire").await.unwrap();
    let mut bytes = [0; 4];
    io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"wire");
    drop(io);
    target.await.unwrap();
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = udp.local_addr().unwrap().port();
    let target = tokio::spawn(async move {
        let mut bytes = [0; 128];
        let (size, peer) = udp.recv_from(&mut bytes).await.unwrap();
        udp.send_to(&bytes[..size], peer).await.unwrap();
    });
    let mut control = tokio::net::TcpStream::connect(socks).await.unwrap();
    control.write_all(&[5, 1, 0]).await.unwrap();
    let mut reply = [0; 2];
    control.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [5, 0]);
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut reply = [0; 10];
    control.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..4], &[5, 0, 0, 1]);
    let relay =
        std::net::SocketAddr::from(([127, 0, 0, 1], u16::from_be_bytes([reply[8], reply[9]])));
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut packet = vec![0, 0, 0, 1, 127, 0, 0, 1];
    packet.extend(port.to_be_bytes());
    packet.extend(b"udp");
    peer.send_to(&packet, relay).await.unwrap();
    let mut received = [0; 128];
    let size = peer.recv(&mut received).await.unwrap();
    assert_eq!(&received[..size], packet);
    drop(control);
    target.await.unwrap();
}
pub(super) async fn headers(io: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(io.read_u8().await.unwrap());
        assert!(bytes.len() < 16384);
    }
    let headers = String::from_utf8(bytes).unwrap();
    let size = headers
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    let mut body = vec![0; size];
    io.read_exact(&mut body).await.unwrap();
    headers
}
