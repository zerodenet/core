use super::*;

fn config(relay: &[&str], prefix: serde_json::Value) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[
            {"tag":"prefix","protocol":prefix},
            {"tag":"pool","protocol":{"type":"mieru","server":"127.0.0.1","port":1234,"password":"p"}},
            {"tag":"other-pool","protocol":{"type":"mieru","server":"127.0.0.1","port":1235,"password":"p"}}
        ],
        "outbound_groups":[{"tag":"relay","type":"relay","proxies":relay}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}

#[test]
#[cfg(feature = "vless")]
fn mixed_legacy_prefix_never_promotes_half_of_the_one_mieru_pool() {
    let config = config(
        &["prefix", "pool"],
        serde_json::json!({
            "type":"vless", "server":"127.0.0.1", "port":443,
            "id":"11111111-2222-3333-4444-555555555555",
            "http_upgrade":{"path":"/transport"}
        }),
    );
    assert!(!production_selected(Some(&config)));
    let proxy = crate::Proxy::new(config).unwrap();
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    for tag in ["pool", "other-pool"] {
        assert!(
            plan.configured(tag).is_none(),
            "the same pool must retain one compatibility owner"
        );
    }
}

#[test]
fn mixed_legacy_suffix_cannot_force_an_already_native_pool_back_to_compatibility() {
    let config = config(
        &["pool", "prefix"],
        serde_json::json!({
            "type":"vless", "server":"127.0.0.1", "port":443,
            "id":"11111111-2222-3333-4444-555555555555",
            "http_upgrade":{"path":"/transport"}
        }),
    );
    assert!(!production_selected(Some(&config)));
}

#[test]
fn unrelated_unmigrated_node_does_not_disable_the_actual_mieru_graph() {
    let config = config(
        &["pool", "other-pool"],
        serde_json::json!({
            "type":"vless", "server":"127.0.0.1", "port":443,
            "id":"11111111-2222-3333-4444-555555555555",
            "http_upgrade":{"path":"/transport"}
        }),
    );
    assert!(production_selected(Some(&config)));
}
