use super::*;
fn input(extra: &str) -> OutboundConfig {
    zero_config::RuntimeConfig::parse(&format!(
        r#"{{"outbounds":[{{"tag":"native","protocol":{{
        "type":"vmess","server":"127.0.0.1","port":443,
        "id":"11111111-2222-3333-4444-555555555555","cipher":"aes-128-gcm"{extra}
    }}}}],"route":{{"final":{{"type":"direct"}}}}}}"#
    ))
    .unwrap()
    .outbounds
    .remove(0)
}
#[test]
fn resourceful_styles_remain_excluded_and_direct_options_never_discard_them() {
    let registry = crate::register::component_registry();
    assert!(registry.has_factory("vmess"));
    for extra in [
        r#", "mux_concurrency":2"#,
        r#", "tls":{"ca_cert_path":"not-opened.pem"}"#,
        r#", "tls":{"server_name":"localhost","options":{"ech_config_list":"udp://127.0.0.1:53"}}"#,
        r#", "ws":{"browser_dialer":{}}"#,
    ] {
        let input = input(extra);
        assert!(!native_input(&input), "{extra}");
        assert!(
            !registry.has_factory_for("vmess", &input),
            "unmigrated configuration retains its legacy owner: {extra}"
        );
        assert!(component_options(&input).is_err(), "{extra}");
    }
}
#[test]
fn ordinary_raw_tls_and_websocket_remain_selected_without_background_carrier_drivers() {
    let registry = crate::register::component_registry();
    for extra in [
        "",
        r#", "tls":{"server_name":"localhost"}"#,
        r#", "ws":{"path":"/native"}"#,
        r#", "tls":{"server_name":"localhost"},"ws":{"path":"/native"}"#,
    ] {
        let input = input(extra);
        assert!(native_input(&input), "{extra}");
        assert!(registry.has_factory_for("vmess", &input), "{extra}");
        assert!(component_options(&input).is_ok(), "{extra}");
    }
}
#[test]
fn an_invalid_selected_native_credential_is_an_error_and_never_a_legacy_fallback() {
    let mut input = input("");
    assert!(native_input(&input));
    assert!(component_options(&input).is_ok());
    let OutboundProtocolConfig::Vmess { id, .. } = &mut input.protocol else {
        unreachable!()
    };
    *id = "invalid".into();
    assert!(native_input(&input));
    assert!(component_options(&input).is_err());
}

#[test]
fn grpc_udp_selects_actual_datagram_without_migrating_tcp_or_legacy_resource_styles() {
    use zero_platform_tokio::contract::component::{
        ComponentFactory, ResourceDeclaration, RoleKind,
    };
    let registry = crate::register::component_registry();
    for extra in [
        r#", "grpc":{"service_names":["native/Tun"],"multi_mode":false}"#,
        r#", "grpc":{"service_names":["native/Tun"],"multi_mode":true}"#,
        r#", "tls":{"server_name":"localhost"},"grpc":{"service_names":["native/Tun"]}"#,
    ] {
        let input = input(extra);
        assert!(!native_input(&input));
        assert!(registry.has_factory_for("vmess", &input));
        let options = super::super::VmessAdapter::default()
            .standard_options(&input)
            .unwrap();
        let spec = grpc::Factory.describe(&options);
        assert_eq!(spec.roles, vec![RoleKind::Datagram]);
        assert!(matches!(
            &spec.resources[..],
            [ResourceDeclaration::ControlledNodeStream { .. }]
        ));
        assert!(spec.task_limit.is_some());
    }
    let mut hostname = input(r#", "grpc":{"service_names":["native/Tun"]}"#);
    let OutboundProtocolConfig::Vmess { server, .. } = &mut hostname.protocol else {
        unreachable!()
    };
    *server = "node.test".into();
    assert!(!registry.has_factory_for("vmess", &hostname));
}

#[test]
fn grpc_datagram_complete_identity_preserves_distinct_literal_server_descriptions() {
    use zero_platform_tokio::contract::component::ComponentFactory;
    let adapter = super::super::VmessAdapter::default();
    let mut first = input(r#", "grpc":{"service_names":["native/Tun"]}"#);
    let OutboundProtocolConfig::Vmess { server, .. } = &mut first.protocol else {
        unreachable!()
    };
    *server = "::1".into();
    let a = adapter.standard_options(&first).unwrap();
    let OutboundProtocolConfig::Vmess { server, .. } = &mut first.protocol else {
        unreachable!()
    };
    *server = "0:0:0:0:0:0:0:1".into();
    let b = adapter.standard_options(&first).unwrap();
    assert_ne!(
        grpc::Factory.describe(&a).static_identity,
        grpc::Factory.describe(&b).static_identity
    );
}

#[test]
fn grpc_ca_production_projection_keeps_candidate_directory_and_file_budget() {
    use zero_platform_tokio::contract::component::{ComponentFactory, ResourceDeclaration};
    let raw = input(
        r#", "grpc":{"service_names":["ca/Tun"]},"tls":{"server_name":"localhost","ca_cert_path":"../ca.pem"}"#,
    );
    let mut config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    config.source_dir = Some("/not-opened/config".into());
    config.runtime.file_read_max_bytes = Some(12345);
    assert!(grpc::eligible(&raw));
    let options = super::super::VmessAdapter::default()
        .standard_options_for(&raw, Some(&config))
        .unwrap();
    let spec = grpc::Factory.describe(&options);
    assert!(
        matches!(&spec.resources[..],[ResourceDeclaration::File{path,max_bytes:12345},ResourceDeclaration::ControlledNodeStream{..},ResourceDeclaration::ControlledCarrierPool{datagram:false,..}] if path==std::path::Path::new("/not-opened/config/../ca.pem"))
    );
}

#[test]
fn grpc_domain_selection_requires_native_node_dns_and_keeps_invalid_credentials_as_errors() {
    let mut hostname = input(r#", "grpc":{"service_names":["native/Tun"]}"#);
    let OutboundProtocolConfig::Vmess { server, .. } = &mut hostname.protocol else {
        unreachable!()
    };
    *server = "node.test".into();
    let mut candidate =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    assert!(!grpc::selected(&hostname, Some(&candidate), true));
    candidate.runtime.dns = Some(zero_config::DnsConfig {
        servers: std::collections::BTreeMap::from([(
            "native".into(),
            zero_config::DnsServerConfig::Udp {
                host: "127.0.0.1".into(),
                port: 53,
                bootstrap: vec![],
                detour: None,
            },
        )]),
        default_server: "native".into(),
        dispatch: vec![],
        reverse_mapping: None,
        answer: zero_config::DnsAnswerConfig::Real,
        cache: None,
        policy: Default::default(),
    });
    assert!(grpc::selected(&hostname, Some(&candidate), true));
    assert!(!grpc::selected(&hostname, Some(&candidate), false));
    let OutboundProtocolConfig::Vmess { id, .. } = &mut hostname.protocol else {
        unreachable!()
    };
    *id = "invalid".into();
    assert!(grpc::selected(&hostname, Some(&candidate), true));
    assert!(super::super::VmessAdapter::default()
        .standard_options_for(&hostname, Some(&candidate))
        .is_err());
    let server = candidate
        .runtime
        .dns
        .as_mut()
        .unwrap()
        .servers
        .get_mut("native")
        .unwrap();
    let zero_config::DnsServerConfig::Udp { detour, .. } = server else {
        unreachable!()
    };
    *detour = Some("via".into());
    assert!(!grpc::selected(&hostname, Some(&candidate), true));
}

#[test]
fn ordinary_mux_prefix_admission_rejects_nonroot_nested_and_unowned_views() {
    let pool = input(r#", "mux_concurrency":2"#);
    let final_node = input(r#", "grpc":{"service_names":["udp"]}"#);
    let mut config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let mut last = final_node;
    last.tag = "final".into();
    config.outbounds = vec![pool.clone(), last];
    config.outbound_groups = serde_json::from_value(
        serde_json::json!([{"tag":"chain","type":"relay","proxies":["native","final"]}]),
    )
    .unwrap();
    let adapter = super::super::VmessAdapter::default();
    assert!(adapter.production_selected(&pool, Some(&config), true));
    assert!(!adapter.production_selected(&pool, Some(&config), false));
    assert!(!adapter.production_selected(&pool, None, true));
    let mut custom_dial = config.clone();
    custom_dial.outbounds[1].dial.interface = Some("lo0".into());
    assert!(!adapter.production_selected(&pool, Some(&custom_dial), true));
    let mut nonroot = config.clone();
    nonroot.outbound_groups.push(
        serde_json::from_value(
            serde_json::json!({"tag":"reverse","type":"relay","proxies":["final","native"]}),
        )
        .unwrap(),
    );
    assert!(!adapter.production_selected(&pool, Some(&nonroot), true));
    let mut nested = config.clone();
    nested.outbound_groups.push(
        serde_json::from_value(
            serde_json::json!({"tag":"nested","type":"relay","proxies":["chain","final"]}),
        )
        .unwrap(),
    );
    assert!(!adapter.production_selected(&pool, Some(&nested), true));
}

#[tokio::test]
async fn ordinary_mux_prefix_does_not_adopt_legacy_io_after_transport_change_or_cache_eviction() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut pool = input(r#", "mux_concurrency":2"#);
    let OutboundProtocolConfig::Vmess { port, .. } = &mut pool.protocol else {
        unreachable!()
    };
    *port = listener.local_addr().unwrap().port();
    let mut last = input(r#", "grpc":{"service_names":["udp"]}"#);
    last.tag = "final".into();
    let mut config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    config.outbounds = vec![pool.clone(), last];
    config.outbound_groups = serde_json::from_value(
        serde_json::json!([{"tag":"chain","type":"relay","proxies":["native","final"]}]),
    )
    .unwrap();
    let adapter = super::super::VmessAdapter::default();
    let OutboundProtocolConfig::Vmess { server, port, .. } = &pool.protocol else {
        unreachable!()
    };
    let refs = super::super::outbound_options(&pool.tag, (server, *port), &pool.protocol).unwrap();
    let leaf =
        ::vmess::transport::VmessOutboundLeaf::from_options_refs(None, refs, &adapter.runtime)
            .unwrap();
    let session = zero_core::Session::new(
        1,
        zero_core::Address::Ipv4([127, 0, 0, 1]),
        53,
        zero_core::Network::Tcp,
        zero_core::ProtocolType::new("legacy"),
    );
    let opened = leaf.open_tcp_stream(&session, |server, port| {
        let server = server.to_owned();
        async move {
            tokio::net::TcpStream::connect((server.as_str(), port))
                .await
                .map(zero_platform_tokio::TokioSocket::new)
                .map_err(Into::into)
        }
    });
    let (opened, accepted) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(opened, listener.accept())
    })
    .await
    .unwrap();
    let opened = opened.unwrap();
    let (actual, _) = accepted.unwrap();
    assert!(!adapter.production_selected(&pool, Some(&config), true));
    drop(opened);
    adapter.runtime.on_config_reloaded();
    let mut changed = pool.clone();
    let OutboundProtocolConfig::Vmess { ws, .. } = &mut changed.protocol else {
        unreachable!()
    };
    *ws = Some(serde_json::from_value(serde_json::json!({"path":"/new"})).unwrap());
    config.outbounds[0] = changed.clone();
    assert!(
        !adapter.production_selected(&changed, Some(&config), true),
        "cache eviction and a new transport are not the old native controller's close proof"
    );
    drop(actual);
    drop(listener);
}
