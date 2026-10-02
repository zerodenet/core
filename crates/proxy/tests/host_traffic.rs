//! Actual OS provider -> runtime -> existing Query/Command/Event contract.
#[cfg(all(
    feature = "host-network-stats",
    any(target_os = "linux", target_os = "macos")
))]
#[tokio::test]
async fn runtime_samples_real_host_interfaces_and_resets_without_touching_flows() {
    use std::time::Duration;
    use zero_api::*;
    use zero_config::RuntimeConfig;
    use zero_engine::EngineHandle;
    use zero_proxy::{Proxy, ProxyHandle};
    let config =
        RuntimeConfig::parse(r#"{"inbounds":[],"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap();
    let proxy = Proxy::new(config).unwrap();
    let engine = proxy.engine().clone();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let stream = handle
        .subscribe(EventFilter {
            event_types: vec![event_type::STATS_HOST_INTERFACES_SAMPLED.into()],
            ..Default::default()
        })
        .unwrap();
    let running = proxy.spawn();
    let sampled = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = stream.try_recv() {
                break event;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let page: TrafficListSnapshot = serde_json::from_value(sampled.payload).unwrap();
    assert!(!page.scopes.is_empty());
    assert!(page
        .scopes
        .iter()
        .all(|s| matches!(s.scope, TrafficScope::HostInterface { .. })));
    let QueryResponse::TrafficStats(queried) = handle
        .query(QueryRequest::TrafficStats(TrafficListQuery {
            include_host_interfaces: true,
            ..Default::default()
        }))
        .unwrap()
    else {
        panic!("expected traffic page");
    };
    let host = queried
        .scopes
        .into_iter()
        .find(|s| matches!(s.scope, TrafficScope::HostInterface { .. }))
        .unwrap();
    assert_eq!(host.planes[0].plane, TrafficPlane::Host);
    assert!(host.planes[0].counters.rx_dropped_packets.is_some());
    #[cfg(target_os = "macos")]
    assert_eq!(host.planes[0].counters.tx_dropped_packets, None);
    let cleared = engine
        .reset_traffic(&StatsResetCommand {
            expected_core_instance_id: host.core_instance_id,
            operation_id: None,
            targets: vec![StatsResetTarget {
                scope: host.scope,
                expected_stats_epoch: host.stats_epoch,
                expected_generation: host.generation,
            }],
        })
        .unwrap();
    assert_eq!(
        cleared.snapshots[0].planes[0].counters.rx_dropped_packets,
        Some(0)
    );
    assert_eq!(engine.stats_snapshot().bytes_up, 0);
    running.shutdown().await.unwrap();
}
