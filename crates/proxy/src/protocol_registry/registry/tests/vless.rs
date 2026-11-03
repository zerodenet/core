//! All lawful VLESS listeners select the actual factory, never a legacy binder.
use serde_json::json;
use zero_config::RuntimeConfig;
fn document(protocol: serde_json::Value) -> serde_json::Value {
    json!({"inbounds":[{"tag":"first","listen":{"address":"127.0.0.1","port":0},
        "protocol":protocol}],"route":{"final":{"type":"direct"}}})
}
#[test]
fn vless_mkcp_and_reality_select_standard_sources_without_legacy_admission() {
    let (private, _) = vless::reality::generate_reality_key_pair();
    for extra in [
        json!({"mkcp":{}}),
        json!({"reality":{"private_key":private,
        "short_ids":["0123456789abcdef"],"server_name":"localhost"}}),
    ] {
        let mut protocol = json!({"type":"vless","users":[]});
        protocol
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let proxy = crate::runtime::Proxy::new(
            RuntimeConfig::parse(&document(protocol).to_string()).unwrap(),
        )
        .unwrap();
        let inbound = proxy.engine().config().inbounds[0].clone();
        let prepared = proxy
            .protocols
            .prepare_inbound_bind(inbound.clone(), None)
            .unwrap()
            .unwrap();
        assert!(prepared
            .into_recipe()
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Inbound));
    }
}
#[test]
fn forbidden_mkcp_reality_and_hysteria_without_quic_remain_config_errors() {
    let (private, _) = vless::reality::generate_reality_key_pair();
    let error = RuntimeConfig::parse(
        &document(json!({"type":"vless","users":[],"mkcp":{},
        "reality":{"private_key":private,"short_ids":["0123456789abcdef"]}}))
        .to_string(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("mKCP is an independent carrier"));
    let error = RuntimeConfig::parse(
        &document(json!({"type":"vless","users":[],
        "hysteria":{"auth":"existing"}}))
        .to_string(),
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("Hysteria carrier requires auth and QUIC TLS settings"));
}
#[test]
fn failed_standard_vless_preparation_cannot_fall_back_to_legacy_listener() {
    let mut config =
        RuntimeConfig::parse(&document(json!({"type":"vless","users":[],"mkcp":{}})).to_string())
            .unwrap();
    let mut inbound = config.inbounds.remove(0);
    let zero_config::InboundProtocolConfig::Vless { tls, .. } = &mut inbound.protocol else {
        unreachable!()
    };
    *tls = Some(Box::new(
        serde_json::from_value(json!({"cert_path":"/nonexistent/zero-test/ca.pem",
        "key_path":"/nonexistent/zero-test/key.pem"}))
        .unwrap(),
    ));
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    assert!(proxy
        .protocols
        .prepare_inbound_bind(inbound.clone(), None)
        .is_err());
}
