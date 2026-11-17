use super::*;
fn config(groups: serde_json::Value) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":443,"id":"11111111-2222-3333-4444-555555555555","grpc":{"service_names":["actual/Tun"]}}},{"tag":"other","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}}],"outbound_groups":groups,"route":{"final":{"type":"direct"}}}).to_string()).unwrap()
}
#[test]
fn same_pool_removes_legacy_claims_only_for_the_whole_owning_configuration() {
    let standalone = config(serde_json::json!([]));
    let node = &standalone.outbounds[0];
    assert!(selected(node, Some(&standalone), true));
    assert!(!selected(node, None, true));
    assert!(!selected(node, Some(&standalone), false));
    let mixed = config(
        serde_json::json!([{ "tag":"choice","type":"selector","outbounds":["other","node"],"selected":"other"},{"tag":"chain","type":"relay","proxies":["other","choice"]}]),
    );
    assert!(whole_selected(&mixed, true));
}
#[test]
fn cold_owner_projects_configured_per_pool_and_registry_budgets_without_starting() {
    use zero_platform_tokio::contract::component::{
        ComponentFactory, ResourceDeclaration, RoleKind,
    };
    let mut config = config(serde_json::json!([]));
    config.runtime.grpc_pool_max_carriers = Some(2);
    config.runtime.grpc_pool_max_streams = Some(3);
    config.runtime.grpc_total_max_carriers = Some(5);
    config.runtime.grpc_total_max_streams = Some(7);
    let adapter = VlessAdapter {
        native_node_dns: true,
        ..Default::default()
    };
    let mut next = 1;
    let mut allocate = || {
        let id = zero_core::component::ComponentId(next);
        next += 1;
        Ok(id)
    };
    let mut resources = ResourcePreparation::new(&mut allocate).with_candidate(&config);
    let factory = Arc::new(pool::Factory::new(Arc::new(
        crate::component::background::BackgroundNetwork::new(Default::default()),
    )));
    let options = adapter
        .production_mux_options(&config.outbounds[0], &mut resources, factory)
        .unwrap();
    let role = super::super::super::mux::Factory.describe(&options);
    assert_eq!(role.roles, vec![RoleKind::Stream, RoleKind::Datagram]);
    assert_eq!(role.task_limit, None);
    assert_eq!(role.dependencies.len(), 1);
    let entries = resources.resources().collect::<Vec<_>>();
    assert_eq!(entries.len(), 2);
    let pool = entries
        .iter()
        .find(|entry| entry.component() == role.dependencies[0])
        .unwrap();
    let spec = pool.recipe().spec();
    assert_eq!(spec.task_limit.unwrap().get(), 2);
    assert_eq!(spec.dependencies.len(), 1);
    assert!(
        matches!(&spec.resources[..],[ResourceDeclaration::ControlledCarrierPool{datagram:false,max_sockets,..}] if max_sockets.get()==2)
    );
    let root = entries
        .iter()
        .find(|entry| entry.component() == spec.dependencies[0])
        .unwrap()
        .recipe()
        .spec();
    assert!(root.roles.is_empty() && root.resources.is_empty() && root.task_limit.is_none());
}

#[test]
fn literal_ech_uses_a_declared_node_query_without_inventing_an_ip_bootstrap_requirement() {
    use zero_platform_tokio::contract::component::{ComponentFactory, ResourceDeclaration};
    let mut config = config(serde_json::json!([]));
    config.runtime.dns=Some(serde_json::from_value(serde_json::json!({"servers":{"query":{"type":"udp","host":"127.0.0.1","port":53,"detour":"node"}},"default_server":"query"})).unwrap());
    assert!(zero_dns::native::validate_node_config(config.runtime.dns.as_ref()).is_err());
    let zero_config::OutboundProtocolConfig::Vless { tls, .. } = &mut config.outbounds[0].protocol
    else {
        unreachable!()
    };
    *tls=Some(serde_json::from_value(serde_json::json!({"server_name":"cover.test","options":{"ech_config_list":"cover.test+udp://127.0.0.1:5354"}})).unwrap());
    assert!(whole_selected(&config, true));
    let adapter = VlessAdapter {
        native_node_dns: true,
        ..Default::default()
    };
    let mut next = 1;
    let mut allocate = || {
        let id = zero_core::component::ComponentId(next);
        next += 1;
        Ok(id)
    };
    let mut resources = ResourcePreparation::new(&mut allocate).with_candidate(&config);
    let factory = Arc::new(pool::Factory::new(Arc::new(
        crate::component::background::BackgroundNetwork::new(Default::default()),
    )));
    let options = adapter
        .production_mux_options(&config.outbounds[0], &mut resources, factory)
        .unwrap();
    let role = super::super::super::mux::Factory.describe(&options);
    let entries = resources.resources().collect::<Vec<_>>();
    let actualpool = entries
        .iter()
        .find(|entry| entry.component() == role.dependencies[0])
        .unwrap()
        .recipe()
        .spec();
    assert_eq!(
        actualpool.dependencies.len(),
        2,
        "same root totals and actual Node query Scope"
    );
    assert!(entries.iter().any(|entry|actualpool.dependencies.contains(&entry.component())&&entry.recipe().spec().resources.iter().any(|resource|matches!(resource,ResourceDeclaration::ControlledCarrierPool{endpoint,datagram:true,..} if endpoint.port==5354))));
    drop(entries);
    drop(resources);
    let zero_config::OutboundProtocolConfig::Vless { tls: Some(tls), .. } =
        &mut config.outbounds[0].protocol
    else {
        unreachable!()
    };
    tls.options.ech_config_list = "cover.test+udp://domain.ech.test:5354".into();
    assert!(
        !whole_selected(&config, true),
        "real Domain ECH bootstrap must use a qualified Node IP backend"
    );
}

#[test]
fn grpc_lower_stream_carriers_prepare_one_actual_pool_without_legacy_claims() {
    for layer in [
        serde_json::json!({"ws":{"path":"/lower"}}),
        serde_json::json!({"h2":{"path":"/lower"}}),
        serde_json::json!({"http_upgrade":{"path":"/lower"}}),
    ] {
        let mut candidate = config(serde_json::json!([]));
        let mut protocol = serde_json::to_value(&candidate.outbounds[0].protocol).unwrap();
        protocol
            .as_object_mut()
            .unwrap()
            .extend(layer.as_object().unwrap().clone());
        candidate.outbounds[0].protocol = serde_json::from_value(protocol).unwrap();
        let adapter = VlessAdapter {
            native_node_dns: true,
            ..Default::default()
        };
        assert!(whole_selected(&candidate, true));
        let mut next = 1;
        let mut allocate = || {
            let id = zero_core::component::ComponentId(next);
            next += 1;
            Ok(id)
        };
        let mut resources = ResourcePreparation::new(&mut allocate).with_candidate(&candidate);
        let factory = Arc::new(pool::Factory::new(Arc::new(
            crate::component::background::BackgroundNetwork::new(Default::default()),
        )));
        let options = adapter
            .production_mux_options(&candidate.outbounds[0], &mut resources, factory)
            .unwrap();
        use zero_platform_tokio::contract::component::ComponentFactory;
        let roles = super::super::super::mux::Factory.describe(&options);
        assert_eq!(roles.dependencies.len(), 1);
        assert_eq!(
            resources.resources().count(),
            2,
            "one actual H2 pool and its existing totals owner"
        );
    }
}
