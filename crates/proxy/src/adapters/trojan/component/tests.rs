use super::*;
fn input(extra: &str) -> OutboundConfig {
    zero_config::RuntimeConfig::parse(&format!(r#"{{"outbounds":[{{"tag":"native","protocol":{{"type":"trojan","server":"127.0.0.1","port":443,"password":"test"{extra}}}}}],"route":{{"final":{{"type":"direct"}}}}}}"#)).unwrap().outbounds.remove(0)
}
#[test]
fn grpc_retains_its_existing_driver_owner_while_ordinary_tls_and_websocket_remain_native() {
    let registry = crate::register::component_registry();
    assert!(registry.has_factory("trojan"));
    for extra in [
        r#", "grpc":{"service_names":["native/Tun"],"multi_mode":false}"#,
        r#", "grpc":{"service_names":["native/Tun"],"multi_mode":true}"#,
    ] {
        let input = input(extra);
        assert!(!native_input(&input), "{extra}");
        assert!(
            !registry.has_factory_for("trojan", &input),
            "unmigrated configuration retains its legacy owner: {extra}"
        );
        assert!(component_options(&input).is_err(), "{extra}");
    }
    for extra in ["", r#", "ws":{"path":"/native"}"#] {
        let input = input(extra);
        assert!(native_input(&input), "{extra}");
        assert!(registry.has_factory_for("trojan", &input), "{extra}");
        assert!(component_options(&input).is_ok(), "{extra}");
    }
}

#[test]
fn browser_resource_options_are_never_silently_projected_into_ordinary_websocket() {
    let config = zero_config::RuntimeConfig::parse(
        r#"{
        "outbounds":[{"tag":"browser","protocol":{
            "type":"trojan","server":"127.0.0.1","port":443,"password":"test",
            "ws":{"path":"/native","browser_dialer":{}}
        }}],"route":{"final":{"type":"direct"}}
    }"#,
    )
    .unwrap();
    let input = &config.outbounds[0];
    assert!(!native_input(input));
    assert!(component_options(input).is_err());
}

#[cfg(feature = "vmess")]
#[test]
fn ordinary_mux_prefix_registration_requires_qualified_root_and_complete_protocol_options() {
    let pool = input(r#", "mux_concurrency":2"#);
    let mut config=zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"last","protocol":{"type":"vmess","server":"127.0.0.1","port":443,"id":"11111111-2222-3333-4444-555555555555","grpc":{"service_names":["udp"]}}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
    config.outbounds.insert(0, pool.clone());
    config.outbound_groups = serde_json::from_value(
        serde_json::json!([{"tag":"chain","type":"relay","proxies":["native","last"]}]),
    )
    .unwrap();
    let adapter = super::super::TrojanAdapter::default();
    assert!(adapter.production_selected(&pool, Some(&config), true));
    assert!(!adapter.production_selected(&pool, Some(&config), false));
    assert!(!adapter.production_selected(&pool, None, true));
    let mut custom = config.clone();
    custom.outbounds[1].dial.interface = Some("lo0".into());
    assert!(!adapter.production_selected(&pool, Some(&custom), true));
    let mut nested = config.clone();
    nested.outbound_groups.push(
        serde_json::from_value(
            serde_json::json!({"tag":"outer","type":"relay","proxies":["last","chain"]}),
        )
        .unwrap(),
    );
    assert!(!adapter.production_selected(&pool, Some(&nested), true));
}

#[test]
fn native_mux_complete_identity_includes_actual_websocket_profile_with_stable_node_fence() {
    use zero_platform_tokio::contract::{component::ComponentFactory, network::UpstreamBinding};
    let adapter = super::super::TrojanAdapter::default();
    let describe = |input: &OutboundConfig| {
        let OutboundProtocolConfig::Trojan { server, port, .. } = &input.protocol else {
            unreachable!()
        };
        let refs =
            super::super::outbound_options(&input.tag, (server, *port), &input.protocol).unwrap();
        let prepared = ::trojan::component::mux::PreparedMux::from_options_refs(
            refs,
            &adapter.mux_pool,
            None,
            UpstreamBinding {
                prefix: vec![],
                dial: Default::default(),
                network_generation: zero_core::component::NetworkGeneration(0),
            },
            std::num::NonZeroUsize::new(2).unwrap(),
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        ::trojan::component::mux::PoolFactory.describe(prepared.pool_options())
    };
    let raw = input(r#", "mux_concurrency":2"#);
    let ws = input(r#", "mux_concurrency":2,"ws":{"path":"/changed"}"#);
    assert_eq!(describe(&raw), describe(&raw));
    assert_ne!(
        describe(&raw).static_identity,
        describe(&ws).static_identity,
        "one logical ownership fence cannot alias different real carrier descriptions"
    );
}
