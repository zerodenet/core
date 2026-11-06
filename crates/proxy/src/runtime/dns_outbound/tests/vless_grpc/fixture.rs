//! The remote endpoint is the real ordinary VLESS gRPC inbound.
use super::*;
const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) struct Remote {
    pub(super) address: std::net::SocketAddr,
    proxy: Proxy,
    running: crate::RunningProxy,
    original: Arc<crate::runtime::inbound_owners::InboundOwner>,
}
impl Remote {
    pub(super) async fn new(multi: bool) -> Self {
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"incoming","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"grpc":{"service_names":["actual.dns/Tun"],"multi_mode":multi}}}],
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
        let address = *original
            .original_listener_address
            .get()
            .expect("actual original Source bound port");
        Self {
            address,
            proxy,
            running,
            original,
        }
    }
    pub(super) async fn close(self) {
        self.running.shutdown().await.unwrap();
        let terminal = self
            .original
            .terminal_for_test()
            .expect("same ordinary inbound Source native/Scope cleanup");
        assert_eq!(terminal.identity(), self.original.identity());
        assert!(terminal.failure().is_none());
        assert!(terminal.task_failures().is_empty());
        assert!(terminal.native_bound_failures().is_empty());
        assert!(
            terminal.joined_tasks() >= 2,
            "main and actual accepted H2 driver joined"
        );
        assert!(self.proxy.engine().active_sessions().is_empty());
    }
}
#[derive(Debug)]
struct NoLegacy;
impl zero_dns::DnsOutboundConnector for NoLegacy {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async { panic!("ordinary VLESS gRPC DNS must use the same host pool") })
    }
}
pub(super) async fn start(
    remote: &Remote,
    multi: bool,
    dns: &crate::dns_fixture::Dns,
) -> (Proxy, crate::RunningProxy) {
    start_at(remote.address, multi, dns).await
}
pub(super) async fn start_at(
    address: std::net::SocketAddr,
    multi: bool,
    dns: &crate::dns_fixture::Dns,
) -> (Proxy, crate::RunningProxy) {
    start_with_node(address, multi, dns, None).await
}
pub(super) async fn start_with_node(
    address: std::net::SocketAddr,
    multi: bool,
    dns: &crate::dns_fixture::Dns,
    node: Option<&crate::dns_fixture::Dns>,
) -> (Proxy, crate::RunningProxy) {
    let mut policy = dns.config();
    let mut backend = dns.server();
    let zero_config::DnsServerConfig::Udp { detour, .. } = &mut backend else {
        unreachable!()
    };
    *detour = Some("grpc".into());
    policy.servers.insert("query".into(), backend);
    policy.default_server = "query".into();
    policy.policy.node_server = Some("native".into());
    if let Some(node) = node {
        policy.servers.insert("native".into(), node.server());
    }
    let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"dns":policy},
        "outbounds":[{"tag":"grpc","protocol":{"type":"vless","server":if node.is_some(){"grpc.actual.node.test"}else{"127.0.0.1"},"port":address.port(),"id":ID,"grpc":{"service_names":["actual.dns/Tun"],"multi_mode":multi}}}],
        "route":{"final":{"type":"direct"}}
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
            if let Some(caller) =
                proxy
                    .resolver
                    .native_query_observations()
                    .into_iter()
                    .find(|caller| {
                        caller.io_pending && caller.original.is_some() && !caller.task.is_joined()
                    })
            {
                break caller;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual VLESS gRPC DNS request is awaiting its real reply")
}
