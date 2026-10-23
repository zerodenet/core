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
        let material = TempMaterial::new("zero-trojan-standard-inbound");
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = material.path("server.crt");
        let key_path = material.path("server.key");
        std::fs::write(&cert_path, certified.cert.pem()).unwrap();
        std::fs::write(&key_path, certified.signing_key.serialize_pem()).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certified.cert.der().clone()).unwrap();
        let port = free_port();
        let config = RuntimeConfig::parse(&serde_json::json!({
            "inbounds": [{"tag":"trojan-in", "listen":{"address":"127.0.0.1","port":port},
                "protocol":{"type":"trojan", "users":[{
                    "password":PASSWORD, "principal_key":principal,
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
pub(super) fn request(password: &str, command: u8, target: SocketAddr) -> Vec<u8> {
    // Independent fixed SHA224 wire vectors, rather than the parser's builder.
    let hash = match password {
        PASSWORD => "b42338ef718bb33b88d3c2bf0cb6130499a1dc05e5c101b3722b09c7",
        "wrong-password" => "c860da892e31176af374c37fb20599f70f6d5428c0e53f45a3787c0e",
        _ => panic!("unknown fixture password"),
    };
    let mut bytes = hash.as_bytes().to_vec();
    bytes.extend_from_slice(b"\r\n");
    bytes.extend_from_slice(&[command, ATYP_IPV4]);
    let std::net::IpAddr::V4(ip) = target.ip() else {
        panic!("v4 fixture")
    };
    bytes.extend_from_slice(&ip.octets());
    bytes.extend_from_slice(&target.port().to_be_bytes());
    bytes.extend_from_slice(b"\r\n");
    bytes
}
pub(super) async fn assert_no_response(client: &mut TlsStream<TcpStream>) {
    let mut response = Vec::new();
    let result = client.read_to_end(&mut response).await;
    assert!(response.is_empty(), "Trojan never invents rejection bytes");
    if let Err(source) = result {
        assert!(matches!(
            source.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
        ));
    }
}
