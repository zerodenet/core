//! One real Kernel DNS caller; the independent HTTP peer issues no ownership.
use super::*;
use zero_traits::IpAddress;
#[derive(Debug)]
struct NoLegacy;
impl zero_dns::DnsOutboundConnector for NoLegacy {
    fn connect(&self, _: String, _: SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async { panic!("mixed DNS must use the actual same Host carrier") })
    }
    fn exchange_datagram(
        &self,
        _: String,
        _: SocketAddr,
        _: Vec<u8>,
    ) -> zero_dns::DnsOutboundDatagramFuture {
        Box::pin(async { panic!("mixed DNS must not execute the old exchange bridge") })
    }
}
pub(super) async fn start(
    up: (usize, SocketAddr),
    down: (usize, SocketAddr),
    ca: &std::path::Path,
    mut backend: zero_config::DnsServerConfig,
) -> Running {
    match &mut backend {
        zero_config::DnsServerConfig::Udp { detour, .. }
        | zero_config::DnsServerConfig::Doh { detour, .. } => *detour = Some("node".into()),
        _ => unreachable!("actual UDP and persistent DoH callers"),
    }
    let mut protocol = half(up.0, up.1, ca);
    protocol["type"] = serde_json::json!("vless");
    protocol["id"] = serde_json::json!(ID);
    protocol["split_http"]["download_settings"] = half(down.0, down.1, ca);
    let client = super::super::fixture::start(serde_json::json!({
        "runtime":{"native_stream_max_originals":ORIGINALS,"dns":{
            "servers":{"node":{"type":"system"},"query":backend},
            "default_server":"query","policy":{"node_server":"node","address_family":"ipv4_only"}
        }},
        "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
        "outbounds":[{"tag":"node","protocol":protocol}],
        "route":{"final":{"type":"direct"}}
    }), "entry").await;
    client
        .proxy
        .resolver
        .set_outbound_connector(std::sync::Arc::new(NoLegacy));
    let plan = client
        .proxy
        .tcp_runtime_services()
        .components
        .clone()
        .unwrap();
    let node = plan.configured("node").unwrap();
    assert_eq!(node.dial, zero_traits::DialPolicy::default());
    assert!(plan.has_cached_stream_provider(node.role_id()));
    assert!(plan.stream_dependencies_supported(node.role_id()));
    client
}
pub(super) fn query(
    client: &Running,
    name: &str,
) -> tokio::task::JoinHandle<std::io::Result<Vec<IpAddress>>> {
    let resolver = client.proxy.resolver.clone();
    let name = name.to_owned();
    tokio::spawn(async move { resolver.resolve_real_type(&name, 1).await })
}
pub(super) fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
pub(super) async fn pending(client: &Running) -> zero_dns::OwnedDnsQueryObservation {
    tokio::time::timeout_at(end(), async {
        loop {
            if let Some(actual) = client
                .proxy
                .resolver
                .native_query_observations()
                .into_iter()
                .find(|actual| actual.io_pending && actual.original.is_some())
            {
                return actual;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
pub(super) async fn leases(client: &Running, expected: usize) {
    tokio::time::timeout_at(end(), async {
        loop {
            let current =
                zero_dns::test_support::doh_pool_witness(&client.proxy.resolver, "query").unwrap();
            assert_eq!(current.registered_driver_tasks, 1);
            if current.active_leases == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
pub(super) fn returned(child: &zero_platform_tokio::controlled_carrier::CarrierOrigin) {
    let closed = child.closed().expect("actual DNS child logical close");
    assert!(closed.matches(child) && !closed.had_native_acquisition());
}
pub(super) fn no_business(client: &Running) {
    assert!(client.proxy.engine().active_sessions().is_empty());
    assert!(client.proxy.engine().completed_sessions().is_empty());
}
