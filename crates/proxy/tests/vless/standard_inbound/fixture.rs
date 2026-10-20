use super::*;
use std::net::SocketAddr;
use zero_platform_tokio::contract::stream::BoxStream;
pub(super) struct Fixture {
    pub(super) probe: Engine,
    pub(super) running: zero_proxy::RunningProxy,
    port: u16,
    tls: Option<TestTlsMaterial>,
}
impl Fixture {
    pub(super) fn new(tls: bool, blocked: bool) -> Self {
        Self::build(tls, blocked, false)
    }
    pub(super) fn anonymous_rates() -> Self {
        Self::build(false, false, true)
    }
    fn build(tls: bool, blocked: bool, anonymous: bool) -> Self {
        support::interop::init_logs("vless=debug");
        let material = tls.then(test_tls_material);
        let mut protocol = serde_json::json!({"type":"vless","users":[{
            "id":USER_ID,"principal_key":(!anonymous).then_some("configured:vless"),
            "up_bps":65536,"down_bps":32768,"device_limit":(!anonymous).then_some(2),
            "quota_remaining_bytes":(!anonymous).then_some(4096),"policy_revision":(!anonymous).then_some(7)
        }]});
        if let Some(tls) = &material {
            protocol["tls"] =
                serde_json::json!({"cert_path":tls.cert_path,"key_path":tls.key_path});
        }
        let route = if tls {
            // Only the captured, actual TLS SNI authorizes this target. A lost
            // SNI cannot fall through to another successful route.
            serde_json::json!({"rules":[{"condition":{"type":"sni","values":["localhost"]},"action":{"type":"direct"}}],"final":{"type":"reject"}})
        } else {
            serde_json::json!({"rules":[],"final":{"type":if blocked {"reject"} else {"direct"}}})
        };
        let port = free_port();
        let config=RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"vless-standard-in","listen":{"address":"127.0.0.1","port":port},"protocol":protocol}],"route":route
        }).to_string()).unwrap();
        let engine = Engine::new(config).unwrap();
        let probe = engine.clone();
        let running = spawn_engine(engine);
        Self {
            probe,
            running,
            port,
            tls: material,
        }
    }
    pub(super) async fn connect(&self) -> (BoxStream, SocketAddr) {
        wait_for_listener(self.port).await;
        let io = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
        let source = io.local_addr().unwrap();
        let io: BoxStream = if let Some(material) = &self.tls {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(material.cert_der.clone()).unwrap();
            let tls = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let connector = TlsConnector::from(Arc::new(tls));
            let name = rustls::pki_types::ServerName::try_from("localhost")
                .unwrap()
                .to_owned();
            Box::pin(connector.connect(name, io).await.unwrap())
        } else {
            Box::pin(io)
        };
        (io, source)
    }
}
pub(super) async fn no_response(io: &mut BoxStream) {
    let mut response = Vec::new();
    let result = io.read_to_end(&mut response).await;
    assert!(
        response.is_empty(),
        "rejection must not send a VLESS success header"
    );
    if let Err(source) = result {
        assert!(matches!(
            source.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
        ));
    }
}
