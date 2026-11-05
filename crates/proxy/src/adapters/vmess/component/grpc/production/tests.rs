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
fn node_streams_require_complete_config_and_exclude_every_relay_alias_branch() {
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
    assert!(!streams_selected(&mixed.outbounds[0], Some(&mixed), true));
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
fn whole_node_projection_removes_legacy_tcp_and_keeps_datagram_only_relay_boundary() {
    use zero_platform_tokio::contract::component::{
        ComponentFactory, ResourceDeclaration, RoleKind,
    };
    let adapter = crate::adapters::vmess::VmessAdapter::default();
    for groups in [
        serde_json::json!([]),
        serde_json::json!([
            {"tag":"chain","type":"relay","proxies":["other","node"]}
        ]),
    ] {
        let config = config(groups);
        let node = &config.outbounds[0];
        let enabled = streams_selected(node, Some(&config), false);
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
                    ResourceDeclaration::ControlledCarrierPool { .. }
                ))
                .count(),
            usize::from(enabled)
        );
        let claim = adapter
            .claim_outbound_leaf_impl(crate::protocol_registry::OutboundLeafInput::Proxy {
                config: &config,
                outbound: node,
                endpoint: ("127.0.0.1", 443),
                source_dir: None,
            })
            .unwrap();
        assert_eq!(claim.tcp.is_none(), enabled);
        assert_eq!(claim.tcp_relay.is_none(), enabled);
    }
}
