use super::*;
pub(super) const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) struct Material {
    pub ca: std::path::PathBuf,
    pub key: std::path::PathBuf,
    _directory: tempfile::TempDir,
}
impl Material {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let ca = directory.path().join("ca.pem");
        let key = directory.path().join("key.pem");
        std::fs::write(&ca, certificate.cert.pem()).unwrap();
        std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
        Self {
            ca,
            key,
            _directory: directory,
        }
    }
}
pub(super) struct Source {
    pub address: std::net::SocketAddr,
    running: crate::RunningProxy,
    original: Arc<crate::runtime::inbound_owners::InboundOwner>,
}
impl Source {
    pub async fn start(document: serde_json::Value, tag: &str) -> Self {
        let config = zero_config::RuntimeConfig::parse(&document.to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let registry = proxy.system_tun_source_registry();
        let running = proxy.spawn();
        let original = tokio::time::timeout_at(deadline(), async {
            loop {
                if let Some(original) = registry.owner(tag) {
                    break original;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout_at(deadline(), original.wait_ready())
            .await
            .unwrap()
            .unwrap();
        let handle = crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy,
        );
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
        Self {
            address: *original.original_listener_address.get().unwrap(),
            running,
            original,
        }
    }
    pub async fn close(self) {
        self.running.shutdown().await.unwrap();
        let closed = self.original.terminal_for_test().unwrap();
        assert_eq!(closed.identity(), self.original.identity());
        assert!(
            closed.failure().is_none()
                && closed.task_failures().is_empty()
                && closed.native_bound_failures().is_empty()
        );
    }
}
pub(super) async fn prefix(borrowed: bool) -> Option<Source> {
    if !borrowed {
        return None;
    }
    Some(Source::start(serde_json::json!({"inbounds":[{"tag":"prefix","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}), "prefix").await)
}
#[derive(Debug)]
struct NoCompatibility;
impl zero_dns::DnsOutboundConnector for NoCompatibility {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async { panic!("actual QUIC DNS cannot use the compatibility connector") })
    }
}
pub(super) async fn client(
    peer: std::net::SocketAddr,
    ca: &std::path::Path,
    prefix: Option<&Source>,
    backend: zero_config::DnsServerConfig,
) -> (Proxy, crate::RunningProxy) {
    client_with_mode(peer, ca, prefix, backend, None).await
}
pub(super) async fn client_with_mode(
    peer: std::net::SocketAddr,
    ca: &std::path::Path,
    prefix: Option<&Source>,
    mut backend: zero_config::DnsServerConfig,
    h3: Option<&str>,
) -> (Proxy, crate::RunningProxy) {
    let detour = if prefix.is_some() { "chain" } else { "node" };
    match &mut backend {
        zero_config::DnsServerConfig::Doh {
            detour: selected, ..
        }
        | zero_config::DnsServerConfig::Udp {
            detour: selected, ..
        } => *selected = Some(detour.into()),
        _ => panic!("actual UDP or persistent DoH backend"),
    }
    let mut nodes = vec![
        serde_json::json!({"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":peer.port(),"id":ID,"quic":{"server_name":"localhost","ca_cert_path":ca}}}),
    ];
    if let Some(mode) = h3 {
        nodes[0]["protocol"]["split_http"] = serde_json::json!({
            "path":"/query-original/", "mode":mode,
            "xmux":{"max_connections":1,"max_concurrency":8}
        });
    }
    if let Some(prefix) = prefix {
        nodes.insert(0, serde_json::json!({"tag":"prefix","protocol":{"type":"socks5","server":"127.0.0.1","port":prefix.address.port()}}));
    }
    let mut document = serde_json::json!({"runtime":{"native_stream_max_originals":1,"native_dns_max_originals":1,"dns":{"servers":{"node":{"type":"system"},"query":backend},"default_server":"query","policy":{"node_server":"node","address_family":"ipv4_only"}}},"outbounds":nodes,"route":{"final":{"type":"direct"}}});
    if prefix.is_some() {
        document["outbound_groups"] =
            serde_json::json!([{"tag":"chain","type":"relay","proxies":["prefix","node"]}]);
    }
    let config = zero_config::RuntimeConfig::parse(&document.to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    proxy
        .resolver
        .set_outbound_connector(Arc::new(NoCompatibility));
    let running = proxy.spawn();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    (proxy, running)
}
