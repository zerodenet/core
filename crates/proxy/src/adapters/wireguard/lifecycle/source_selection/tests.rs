use super::*;
fn configuration() -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(r#"{
        "inbounds":[],
        "outbounds":[
            {"tag":"outer","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}},
            {"tag":"wg","protocol":{"type":"wireguard","private_key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=","addresses":["10.0.0.2/32"],"outer_udp_proxy":"outer","peers":[{"public_key":"AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=","endpoint":"127.0.0.1:51820","allowed_ips":["0.0.0.0/0"]}]}}
        ],"route":{"final":{"type":"route","outbound":"wg"}}
    }"#).unwrap()
}
#[cfg(feature = "socks5")]
#[test]
fn same_registered_factory_selects_outer_before_any_material_or_original_is_prepared() {
    let config = configuration();
    let (_, factories, _) = crate::register::compiled_registries_with_background(None);
    let actual = &config.outbounds[1];
    assert!(outer_members_selected(actual, &config, |member| {
        factories.has_factory_for_candidate(member.protocol.protocol_name(), member, Some(&config))
    }));
    // The same configuration and path metadata do not grant a missing factory.
    let empty = zero_runtime::component::ComponentRegistry::default();
    assert!(!outer_members_selected(actual, &config, |member| {
        empty.has_factory_for_candidate(member.protocol.protocol_name(), member, Some(&config))
    }));
}
#[test]
fn flat_relay_requires_each_actual_member_and_never_reuses_a_previous_acceptance() {
    let mut config = configuration();
    config.outbound_groups = serde_json::from_value(serde_json::json!([
        {"tag":"chain","type":"relay","proxies":["outer","wg"]}
    ]))
    .unwrap();
    if let OutboundProtocolConfig::Wireguard {
        outer_udp_proxy, ..
    } = &mut config.outbounds[1].protocol
    {
        *outer_udp_proxy = Some("chain".to_owned());
    }
    let mut visited = Vec::new();
    assert!(!outer_members_selected(
        &config.outbounds[1],
        &config,
        |member| {
            visited.push(member.tag.clone());
            member.tag == "outer"
        }
    ));
    assert_eq!(visited, ["outer", "wg"]);
    config.outbound_groups[0].group = zero_config::OutboundGroupKind::Relay {
        proxies: vec!["outer".into(), "missing".into()],
    };
    assert!(!outer_members_selected(
        &config.outbounds[1],
        &config,
        |_| true
    ));
}
