//! Absence of explicit DNS retains the real host-owned System Node resolver.
use crate::runtime::Proxy;
use std::time::Duration;
use zero_platform_tokio::contract::{
    dns::{DnsQueryContext, DnsQueryRole, DnsRequest},
    CancellationToken,
};
use zero_traits::IpAddress;
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
fn context() -> DnsQueryContext {
    DnsQueryContext {
        deadline: Some(end()),
        cancel: CancellationToken::new(),
        expected_generation: None,
    }
}
#[tokio::test]
async fn ordinary_implicit_system_node_uses_owned_blocking_capacity_and_stops_with_its_host() {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    assert!(config.runtime.dns.is_none());
    let proxy = Proxy::new(config.clone()).unwrap();
    let node = proxy.resolver.native_node_service();
    assert!(
        node.resolve(DnsRequest::new("127.0.0.1", DnsQueryRole::Node), context())
            .await
            .is_err(),
        "cold native Node work still needs its actual host"
    );
    assert!(proxy.resolver.native_query_observations().is_empty());
    let running = proxy.spawn();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let generation = node.generation();
    for _ in 0..crate::component::native_dns::LIMIT + 2 {
        // This is the actual existing System blocking lookup input. Unlike
        // localhost, a numeric host does not take the RFC6761 local short path.
        let resolution = node
            .resolve(DnsRequest::new("127.0.0.1", DnsQueryRole::Node), context())
            .await
            .unwrap();
        assert_eq!(resolution.addresses, vec![IpAddress::V4([127, 0, 0, 1])]);
        assert_eq!(resolution.generation, generation);
    }
    assert!(
        proxy
            .resolver
            .native_query_observations()
            .iter()
            .all(|original| original.original.is_none() && original.metadata.is_none()),
        "OS work/task joins do not fabricate native socket receipts"
    );
    assert!(
        proxy.engine.active_sessions().is_empty() && proxy.engine.completed_sessions().is_empty()
    );
    running.shutdown().await.unwrap();
    assert!(node
        .resolve(DnsRequest::new("127.0.0.1", DnsQueryRole::Node), context())
        .await
        .is_err());
    assert!(proxy.resolver.native_query_observations().is_empty());
}
