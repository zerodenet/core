//! One actual Default journal survives native/Scope replacement and restart.
use crate::{Proxy, ProxyHandle};
use std::{path::Path, time::Duration};
use zero_config::RuntimeConfig;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::component::{ResourceDeclaration, ResourceOwner};
use zero_traits::DnsResolver;
fn config(directory: &Path, limit: usize) -> RuntimeConfig {
    RuntimeConfig::parse_with_source_dir(
        &serde_json::json!({
            "runtime":{"file_read_max_bytes":limit,"dns":{
                "servers":{"system":{"type":"system"}},"default_server":"system",
                "answer":{"type":"fake_ip","cidr":"198.18.0.0/24",
                    "ttl_seconds":3600,"max_entries":16}
            }},"route":{"final":{"type":"direct"}}
        })
        .to_string(),
        Some(directory.to_owned()),
    )
    .unwrap()
}
fn handle(proxy: &Proxy) -> ProxyHandle {
    ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    )
}
fn instances(
    proxy: &Proxy,
    path: &Path,
    limit: usize,
) -> (ComponentInstanceId, ComponentInstanceId) {
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let mut query = None;
    let mut node = None;
    for instance in proxy.resolver.native_donor_instances().unwrap() {
        let slot = plan.component(instance.component).unwrap();
        assert_eq!(slot.instance_id(), instance);
        let spec = slot.recipe.spec();
        let journals: Vec<_> = spec
            .resources
            .iter()
            .filter_map(|declaration| match declaration {
                ResourceDeclaration::ControlledJournal { spec } => Some(spec),
                _ => None,
            })
            .collect();
        match &spec.owner {
            ResourceOwner::Endpoint(name) if name.as_ref() == "proxy.dns_queries" => {
                assert_eq!(journals.len(), 1);
                assert_eq!(journals[0].path, path);
                assert_eq!(journals[0].max_bytes.get(), limit);
                assert!(journals[0].parent_private);
                query = Some(instance);
            }
            ResourceOwner::Endpoint(name) if name.as_ref() == "proxy.dns_node_queries" => {
                assert!(
                    journals.is_empty(),
                    "Node must not open a second journal/lock"
                );
                node = Some(instance);
            }
            _ => panic!("unexpected actual DNS donor {instance:?}"),
        }
    }
    (query.unwrap(), node.unwrap())
}
#[tokio::test]
async fn ordinary_default_journal_is_cold_bounded_and_replaced_only_after_original_scope_ack() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let initial = config(directory.path(), 4096);
        let path = zero_dns::default_fake_ip_state_path(directory.path());
        let proxy = Proxy::new(initial.clone()).unwrap();
        let root = proxy
            .protocols
            .runtime_host()
            .original_budgets()
            .carrier
            .clone();
        assert_eq!(root.snapshot().charged, 0);
        assert!(
            !path.exists(),
            "cold Proxy must not open the genuine library owner"
        );
        assert!(proxy.resolver.native_donor_instances().unwrap().is_empty());
        let running = proxy.spawn();
        let handle = handle(&proxy);
        handle
            .apply_runtime_config_and_wait(initial.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        let (old, node) = instances(&proxy, &path, 4096);
        assert_eq!(root.retained_for_instance(old), 1);
        assert_eq!(root.retained_for_instance(node), 0);
        assert_eq!(
            root.snapshot().charged,
            1,
            "custody graph records, not FD count"
        );
        let assigned = proxy.resolver.resolve("kept.actual.example").await.unwrap();
        assert_eq!(assigned.len(), 1);
        let bytes = std::fs::read(&path).unwrap();
        assert!(!bytes.is_empty() && bytes.len() <= 4096);
        let rejected = config(directory.path(), 1);
        assert!(
            handle
                .apply_runtime_config_and_wait(rejected, Duration::from_secs(5))
                .await
                .is_err(),
            "shrinking below valid history cannot evict it or retire its active original"
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(root.retained_for_instance(old), 1);
        assert_eq!(
            proxy.resolver.resolve("kept.actual.example").await.unwrap(),
            assigned
        );
        let stats = proxy.resolver.fake_ip_stats().await.unwrap();
        assert_eq!(
            (
                stats.live_mappings,
                stats.retired_addresses,
                stats.evictions
            ),
            (1, 0, 0)
        );
        let grown = config(directory.path(), 8192);
        handle
            .apply_runtime_config_and_wait(grown.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        let (fresh, same_node) = instances(&proxy, &path, 8192);
        assert_eq!(fresh.component, old.component);
        assert_ne!(fresh, old);
        assert_eq!(
            same_node, node,
            "the unchanged Node scope has no journal dependency"
        );
        assert_eq!(
            root.retained_for_instance(old),
            0,
            "old matching native/users and actual Scope registration ACK precede refund"
        );
        assert_eq!(root.retained_for_instance(fresh), 1);
        assert_eq!(root.snapshot().charged, 1);
        assert_eq!(
            proxy.resolver.resolve("kept.actual.example").await.unwrap(),
            assigned
        );
        assert!(proxy.engine().active_sessions().is_empty());
        running.shutdown().await.unwrap();
        assert_eq!(root.retained_for_instance(fresh), 0);
        assert_eq!(root.snapshot().charged, 0);
        // A process rebuild uses a new genuine Host only after the old graph's
        // native and Scope debts have actually retired; no concurrent lock owner.
        let restarted = Proxy::new(grown.clone()).unwrap();
        let next_root = restarted
            .protocols
            .runtime_host()
            .original_budgets()
            .carrier
            .clone();
        assert_eq!(next_root.snapshot().charged, 0);
        let next_running = restarted.spawn();
        self::handle(&restarted)
            .apply_runtime_config_and_wait(grown, Duration::from_secs(5))
            .await
            .unwrap();
        let (current, current_node) = instances(&restarted, &path, 8192);
        assert_eq!(next_root.retained_for_instance(current), 1);
        assert_eq!(next_root.retained_for_instance(current_node), 0);
        assert_eq!(
            restarted
                .resolver
                .resolve("kept.actual.example")
                .await
                .unwrap(),
            assigned
        );
        assert_eq!(
            restarted
                .resolver
                .lookup_fake_ip(&assigned[0])
                .await
                .as_deref(),
            Some("kept.actual.example")
        );
        next_running.shutdown().await.unwrap();
        assert_eq!(next_root.snapshot().charged, 0);
        // Remove only this fixture's durable artifacts after actual closure.
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(path.with_extension("lock")).unwrap();
    })
    .await
    .unwrap();
}
