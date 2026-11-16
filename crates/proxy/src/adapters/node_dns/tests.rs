use super::*;
fn config(node: &str, detour: &str) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"dns":{"servers":{"node":{"type":"tcp","host":"203.0.113.53","port":53,"detour":detour}},"default_server":"node"}},
        "outbounds":[{"tag":"donor","protocol":{"type":"socks5","server":node,"port":1080}}],
        "outbound_groups":[{"tag":"choice","type":"fallback","outbounds":["direct","donor"]}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
#[test]
fn node_detour_reuses_independent_direct_and_literal_donors_in_every_group_branch() {
    for tag in ["direct", "donor", "choice"] {
        let config = config("127.0.0.1", tag);
        assert!(validate(&config).is_ok(), "{tag}");
        assert!(
            zero_dns::native::validate_node_config(config.runtime.dns.as_ref()).is_err(),
            "standalone validation has no host detour provider"
        );
    }
    assert!(
        validate(&config("requires.node.test", "choice")).is_err(),
        "an independent fallback must not hide a recursively bootstrapped donor"
    );
}
#[test]
fn selected_node_policy_ignores_unrelated_default_detour_bootstrap() {
    let mut config = config("requires.node.test", "donor");
    let dns = config.runtime.dns.as_mut().unwrap();
    dns.servers
        .insert("independent".into(), zero_config::DnsServerConfig::System);
    dns.policy.node_server = Some("independent".into());
    assert!(validate(&config).is_ok());
}
#[test]
fn node_detour_cycles_are_reported_before_recursing_or_opening_resources() {
    let mut config = config("127.0.0.1", "choice");
    config.outbound_groups[0].group = serde_json::from_value(serde_json::json!({
        "type":"relay","proxies":["choice"]
    }))
    .unwrap();
    let error = validate(&config).unwrap_err();
    assert!(error.to_string().contains("cycle"));
}
#[cfg(all(feature = "vless", feature = "socks5"))]
#[test]
fn domain_vless_with_node_detour_selects_its_actual_factory_without_a_legacy_claim() {
    let mut config = config("127.0.0.1", "direct");
    let node: zero_config::OutboundConfig = serde_json::from_value(serde_json::json!({
        "tag":"consumer","protocol":{"type":"vless","server":"node.actual.test","port":443,
        "id":"11111111-2222-3333-4444-555555555555","grpc":{"service_names":["actual/Tun"]}}
    }))
    .unwrap();
    config.outbounds.push(node);
    let proxy = crate::Proxy::new(config.clone()).unwrap();
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let slot = plan
        .configured("consumer")
        .expect("actual domain gRPC factory");
    assert!(plan.has_stream_loan_provider(slot.role_id()));
    assert!(crate::register::protocol_registry()
        .claim_outbound_leaf(
            &config,
            zero_engine::ResolvedLeafOutbound::Proxy {
                identity: zero_engine::OutboundIdentity::from_config_index(1)
            }
        )
        .is_err());
}
