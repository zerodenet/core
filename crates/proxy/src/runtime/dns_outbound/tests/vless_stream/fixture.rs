//! One normal Source and the same configured per-connection material/native owner.
use super::*;
const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) struct Remote {
    pub(super) address: std::net::SocketAddr,
    pub(super) ca: std::path::PathBuf,
    _material: tempfile::TempDir,
    proxy: Proxy,
    running: crate::RunningProxy,
    original: Arc<crate::runtime::inbound_owners::InboundOwner>,
}
fn transport() -> serde_json::Value {
    serde_json::json!({"host":"front.actual.test","path":"/actual-dns"})
}
impl Remote {
    pub(super) async fn new(kind: &str) -> Self {
        let material = tempfile::tempdir().unwrap();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let ca = material.path().join("tls.pem");
        let key = material.path().join("tls.key");
        std::fs::write(&ca, certificate.cert.pem()).unwrap();
        std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
        let mut protocol = serde_json::json!({"type":"vless","users":[{"id":ID}],"tls":{"cert_path":ca,"key_path":key}});
        protocol[kind] = transport();
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"incoming","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let registry = proxy.system_tun_source_registry();
        let running = proxy.spawn();
        let original = tokio::time::timeout_at(deadline(), async {
            loop {
                if let Some(original) = registry.owner("incoming") {
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
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(3))
            .await
            .unwrap();
        let address = *original.original_listener_address.get().unwrap();
        Self {
            address,
            ca,
            _material: material,
            proxy,
            running,
            original,
        }
    }
    pub(super) async fn close(self) {
        self.running.shutdown().await.unwrap();
        let terminal = self.original.terminal_for_test().unwrap();
        assert_eq!(terminal.identity(), self.original.identity());
        assert!(
            terminal.failure().is_none()
                && terminal.task_failures().is_empty()
                && terminal.native_bound_failures().is_empty()
        );
        assert!(terminal.closed_io_receipts() > 0 && terminal.joined_tasks() >= 2);
        assert!(self.proxy.engine().active_sessions().is_empty());
    }
}
#[derive(Debug)]
struct NoLegacy;
impl zero_dns::DnsOutboundConnector for NoLegacy {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async {
            panic!("ordinary H2/upgrade DNS must use the actual same configured owner")
        })
    }
}
pub(super) async fn start(
    remote: &Remote,
    kind: &str,
    backend: zero_config::DnsServerConfig,
) -> (Proxy, crate::RunningProxy) {
    start_at(remote.address, &remote.ca, kind, backend).await
}
pub(super) async fn start_with_node(
    remote: &Remote,
    kind: &str,
    backend: zero_config::DnsServerConfig,
    node: &crate::dns_fixture::Dns,
) -> (Proxy, crate::RunningProxy) {
    start_config(remote.address, &remote.ca, kind, backend, Some(node)).await
}
pub(super) async fn start_at(
    address: std::net::SocketAddr,
    ca: &std::path::Path,
    kind: &str,
    backend: zero_config::DnsServerConfig,
) -> (Proxy, crate::RunningProxy) {
    start_config(address, ca, kind, backend, None).await
}
async fn start_config(
    address: std::net::SocketAddr,
    ca: &std::path::Path,
    kind: &str,
    mut backend: zero_config::DnsServerConfig,
    node: Option<&crate::dns_fixture::Dns>,
) -> (Proxy, crate::RunningProxy) {
    match &mut backend {
        zero_config::DnsServerConfig::Udp { detour, .. }
        | zero_config::DnsServerConfig::Dot { detour, .. }
        | zero_config::DnsServerConfig::Doh { detour, .. } => *detour = Some("node".into()),
        _ => panic!("existing framed UDP/DoT/DoH detours only"),
    }
    let mut protocol = serde_json::json!({"type":"vless","server":if node.is_some(){"transport.actual.node.test"}else{"127.0.0.1"},"port":address.port(),"id":ID,"tls":{"server_name":"localhost","ca_cert_path":ca}});
    protocol[kind] = transport();
    let node_server = node.map_or_else(
        || serde_json::json!({"type":"system"}),
        |node| serde_json::to_value(node.server()).unwrap(),
    );
    let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"native_stream_max_originals":1,"dns":{"servers":{"node":node_server,"query":backend},"default_server":"query","policy":{"node_server":"node","address_family":"ipv4_only"}}},
        "outbounds":[{"tag":"node","protocol":protocol}],"route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    proxy.resolver.set_outbound_connector(Arc::new(NoLegacy));
    let running = proxy.spawn();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let services = proxy.tcp_runtime_services();
    let slot = services
        .components
        .as_ref()
        .unwrap()
        .configured("node")
        .unwrap();
    assert!(
        slot.recipe.spec().task_limit.is_none(),
        "inline per-connection transport must not invent a driver task"
    );
    (proxy, running)
}
pub(super) fn query(
    proxy: &Proxy,
    name: &str,
) -> tokio::task::JoinHandle<std::io::Result<Vec<IpAddress>>> {
    let resolver = proxy.resolver.clone();
    let name = name.to_owned();
    tokio::spawn(async move { resolver.resolve_real_type(&name, 1).await })
}
pub(super) async fn pending(proxy: &Proxy) -> zero_dns::OwnedDnsQueryObservation {
    tokio::time::timeout_at(deadline(), async {
        loop {
            if let Some(actual) = proxy
                .resolver
                .native_query_observations()
                .into_iter()
                .find(|actual| actual.io_pending && actual.original.is_some())
            {
                break actual;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
pub(super) fn donor(child: &CarrierOrigin) -> CarrierOrigin {
    let donor = child.parent().expect("same per-connection donor");
    assert!(donor.driver().is_none() && donor.parent().is_none());
    assert!(donor.closed().is_none());
    let endpoints = donor
        .endpoints()
        .expect("actual connected native endpoints");
    assert!(endpoints.peer().ip().is_loopback());
    donor.clone()
}
pub(super) fn native_closed(donor: &CarrierOrigin) {
    let closed = donor.closed().expect("actual per-connection native close");
    assert!(closed.matches(donor) && closed.had_native_acquisition());
    assert!(donor.driver().is_none());
}
pub(super) fn returned(child: &CarrierOrigin) {
    let closed = child
        .closed()
        .expect("actual DNS query child logical close");
    assert!(closed.matches(child) && !closed.had_native_acquisition());
}
