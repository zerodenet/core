use super::*;
fn config(groups: serde_json::Value) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[
            {"tag":"node","protocol":{"type":"vmess","server":"127.0.0.1","port":443,"id":"11111111-2222-3333-4444-555555555555","grpc":{"service_names":["actual/Tun"]}}},
            {"tag":"other","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}}
        ],"outbound_groups":groups,"route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
#[test]
fn node_streams_require_complete_config_and_route_every_relay_alias_to_the_same_owner() {
    let standalone = config(serde_json::json!([]));
    assert!(!streams_selected(&standalone.outbounds[0], None, true));
    assert!(streams_selected(
        &standalone.outbounds[0],
        Some(&standalone),
        true
    ));
    // Current selection deliberately avoids node; all legal future selections
    // still matter to the one owning configuration's protocol executor.
    let mixed = config(serde_json::json!([
        {"tag":"choice","type":"selector","outbounds":["other","node"],"selected":"other"},
        {"tag":"backup","type":"fallback","outbounds":["choice"]},
        {"tag":"chain","type":"relay","proxies":["other","backup"]}
    ]));
    assert!(streams_selected(&mixed.outbounds[0], Some(&mixed), true));
    assert!(!streams_selected(&mixed.outbounds[0], Some(&mixed), false));
    let unrelated = config(serde_json::json!([
        {"tag":"choice","type":"selector","outbounds":["node"]},
        {"tag":"chain","type":"relay","proxies":["other","other"]}
    ]));
    assert!(streams_selected(
        &unrelated.outbounds[0],
        Some(&unrelated),
        true
    ));
}
#[test]
fn whole_node_projection_removes_legacy_tcp_for_native_relay_and_single_nodes() {
    use zero_platform_tokio::contract::component::{
        ComponentFactory, ResourceDeclaration, RoleKind,
    };
    let adapter = crate::adapters::vmess::VmessAdapter {
        native_node_dns: true,
        ..Default::default()
    };
    for groups in [
        serde_json::json!([]),
        serde_json::json!([
            {"tag":"chain","type":"relay","proxies":["other","node"]}
        ]),
    ] {
        let config = config(groups);
        let node = &config.outbounds[0];
        let enabled = streams_selected(node, Some(&config), true);
        let options = adapter.standard_options_for(node, Some(&config)).unwrap();
        let spec = Factory.describe(&options);
        assert_eq!(spec.roles.contains(&RoleKind::Stream), enabled);
        assert!(spec.roles.contains(&RoleKind::Datagram));
        assert_eq!(spec.task_limit.unwrap().get(), 4096);
        assert_eq!(
            spec.resources
                .iter()
                .filter(|resource| matches!(
                    resource,
                    ResourceDeclaration::ControlledServiceStream { .. }
                ))
                .count(),
            usize::from(enabled)
        );
        assert!(enabled);
    }
}

#[test]
fn grpc_mux_relay_and_plain_grpc_nodes_share_the_same_native_registry_budget() {
    let mut config = config(serde_json::json!([]));
    let mut legacy = config.outbounds[0].clone();
    legacy.tag = "legacy-node".into();
    if let OutboundProtocolConfig::Vmess {
        mux_concurrency, ..
    } = &mut legacy.protocol
    {
        *mux_concurrency = Some(8);
    }
    config.outbounds.push(legacy);
    config.outbound_groups.push(
        serde_json::from_value(serde_json::json!({
            "tag":"unused-chain","type":"relay","proxies":["legacy-node","other"]
        }))
        .unwrap(),
    );
    for node in config
        .outbounds
        .iter()
        .filter(|node| node.protocol.protocol_name() == "vmess")
    {
        assert!(streams_selected(node, Some(&config), true));
    }
}
