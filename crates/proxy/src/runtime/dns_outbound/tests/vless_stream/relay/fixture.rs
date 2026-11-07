//! One normal remote Source and one actual client Host, no second DNS connector.
use super::*;
#[derive(Debug)]
struct NoCompatibility;
impl zero_dns::DnsOutboundConnector for NoCompatibility {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async { panic!("actual three-role DNS Relay entered the old executor") })
    }
}
pub(super) async fn start(
    remote: &Remote,
    multi: bool,
    mut backend: zero_config::DnsServerConfig,
) -> (Proxy, crate::RunningProxy) {
    match &mut backend {
        zero_config::DnsServerConfig::Udp { detour, .. }
        | zero_config::DnsServerConfig::Doh { detour, .. } => *detour = Some("chain".into()),
        _ => unreachable!("existing framed UDP and persistent DoH consumers"),
    }
    let nodes: Vec<_> = (0..3).map(|index| serde_json::json!({
        "tag":format!("node-{index}"),"protocol":{"type":"vmess","server":"127.0.0.1","port":remote.address.port(),
        "id":"11111111-2222-3333-4444-555555555555","cipher":"aes-128-gcm",
        "tls":{"server_name":"localhost","ca_cert_path":remote.ca,"client_fingerprint":null},
        "grpc":{"service_names":["actual.dns/Tun"],"multi_mode":multi}}
    })).collect();
    let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"vmess_grpc_max_originals":1,"dns":{"servers":{"node":{"type":"system"},"query":backend},"default_server":"query","policy":{"node_server":"node","address_family":"ipv4_only"}}},
        "outbounds":nodes,"outbound_groups":[{"tag":"chain","type":"relay","proxies":["node-0","node-1","node-2"]}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
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
pub(super) async fn leases(proxy: &Proxy, count: usize) {
    tokio::time::timeout_at(deadline(), async {
        loop {
            let state = zero_dns::test_support::doh_pool_witness(&proxy.resolver, "query").unwrap();
            assert_eq!(state.registered_driver_tasks, 1);
            if state.active_leases == count {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
pub(super) fn no_business(proxy: &Proxy) {
    assert!(proxy.engine().active_sessions().is_empty());
    assert!(proxy.engine().completed_sessions().is_empty());
}
