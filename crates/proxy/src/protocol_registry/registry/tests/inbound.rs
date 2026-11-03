use super::fixtures::{compiled_in_inbound_configs, inbound_protocol_name};

#[test]
fn builtin_fixed_tcp_selects_actual_standard_factory_and_rejects_old_preparation() {
    let proxy = crate::runtime::Proxy::new(
        zero_config::RuntimeConfig::parse(
            r#"{
        "inbounds":[{"tag":"entry", "listen":{"address":"localhost","port":0},
            "udp":{"enabled":false},"protocol":{"type":"direct","target":"example.org"}}],
        "route":{"final":{"type":"direct"}}
    }"#,
        )
        .unwrap(),
    )
    .unwrap();
    let input = proxy.engine().config().inbounds[0].clone();
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(input.clone(), None)
        .unwrap()
        .unwrap();
    assert!(prepared
        .into_recipe()
        .spec()
        .roles
        .contains(&zero_platform_tokio::contract::component::RoleKind::Inbound));
    #[cfg(feature = "managed-datagram-runtime")]
    {
        let mut udp = input;
        udp.udp.enabled = true;
        let prepared = proxy
            .protocols
            .prepare_inbound_bind(udp.clone(), None)
            .unwrap()
            .unwrap();
        assert!(matches!(
            prepared.resources(),
            [
                zero_platform_tokio::contract::component::ResourceDeclaration::TcpListener { .. },
                zero_platform_tokio::contract::component::ResourceDeclaration::DatagramListener {
                    accepted: Some(_),
                    ..
                }
            ]
        ));
    }
}

#[test]
fn compiled_in_inbound_variants_have_exactly_one_registered_adapter() {
    let registry = crate::register::protocol_registry();

    for config in compiled_in_inbound_configs() {
        let claim_count = registry
            .entries
            .iter()
            .filter(|entry| entry.support.supports_inbound(&config))
            .count();
        assert_eq!(
            claim_count,
            1,
            "{} inbound config should be claimed by exactly one adapter",
            inbound_protocol_name(&config)
        );
        let executable = registry.inbounds.has_factory(config.protocol_name());
        assert!(
            executable,
            "{} inbound config should resolve through its actual source or remaining listener",
            inbound_protocol_name(&config)
        );
    }
}
