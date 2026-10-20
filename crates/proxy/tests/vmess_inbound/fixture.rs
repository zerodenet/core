use super::*;
use std::net::SocketAddr;
use tokio_rustls::{client::TlsStream, TlsConnector};
use zero_proxy::RunningProxy;

pub(super) struct Fixture {
    pub(super) probe: Engine,
    pub(super) running: RunningProxy,
    port: u16,
    connector: TlsConnector,
    _material: TempMaterial,
}
impl Fixture {
    pub(super) fn new(principal: Option<&str>, blocked: bool) -> Self {
        let material = TempMaterial::new("zero-vmess-standard-inbound");
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = material.path("server.crt");
        let key_path = material.path("server.key");
        std::fs::write(&cert_path, certified.cert.pem()).unwrap();
        std::fs::write(&key_path, certified.signing_key.serialize_pem()).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certified.cert.der().clone()).unwrap();
        let port = free_port();
        let config = RuntimeConfig::parse(&serde_json::json!({
            "inbounds": [{"tag":"vmess-in", "listen":{"address":"127.0.0.1","port":port},
                "protocol":{"type":"vmess", "users":[{
                    "id":USER_ID, "cipher":"chacha20-poly1305", "principal_key":principal,
                    "up_bps":65536,"down_bps":32768,"device_limit":2,
                    "quota_remaining_bytes":4096,"policy_revision":7
                }], "tls":{"cert_path":cert_path,"key_path":key_path}}}],
            "route":{"rules":[], "final": if blocked { serde_json::json!({"type":"reject"}) } else { serde_json::json!({"type":"direct"}) }}
        }).to_string()).unwrap();
        let engine = Engine::new(config).unwrap();
        let probe = engine.clone();
        let running = spawn_engine(engine);
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(tls));
        Self {
            probe,
            running,
            port,
            connector,
            _material: material,
        }
    }
    pub(super) async fn connect(&self) -> (TlsStream<TcpStream>, SocketAddr) {
        wait_for_listener(self.port).await;
        let socket = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        let source = socket.local_addr().unwrap();
        let name = rustls::pki_types::ServerName::try_from("localhost")
            .unwrap()
            .to_owned();
        let tls = self.connector.connect(name, socket).await.unwrap();
        (tls, source)
    }
}
pub(super) async fn vmess_client(
    fixture: &Fixture,
    target: SocketAddr,
) -> (
    impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    SocketAddr,
) {
    let (tls, source) = fixture.connect().await;
    // This is the client wire builder, not an inbound host Session.
    let request = zero_core::Session::new(
        901,
        match target.ip() {
            std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
        },
        target.port(),
        zero_core::Network::Tcp,
        zero_core::ProtocolType::new("client"),
    );
    let wire = ::vmess::outbound::PreparedVmessOutboundRequestBundle::from_config(
        USER_ID,
        "chacha20-poly1305",
        None,
    )
    .unwrap();
    let (client, _) = wire
        .establish_tcp_outbound_stream(zero_platform_tokio::TcpRelayStream::new(tls), &request)
        .await
        .unwrap();
    (client, source)
}
