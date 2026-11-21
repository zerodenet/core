//! Actual immutable cold data survives the real no-build network refresh.
use super::*;
struct Fixture {
    config: Arc<zero_config::RuntimeConfig>,
    registry: crate::protocol_registry::ProtocolRegistry,
    devices: Arc<crate::adapters::devices::Devices>,
    host: crate::runtime::RuntimeHost,
    network: Arc<crate::component::background::BackgroundNetwork>,
}
impl Fixture {
    fn new() -> Self {
        let mut config = serde_json::to_value(configuration()).unwrap();
        config["outbounds"][0]["protocol"]["outer_udp_proxy"] = serde_json::json!("outer-chain");
        config["outbounds"].as_array_mut().unwrap().extend([
            serde_json::json!({"tag":"outer-socks","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}}),
            serde_json::json!({"tag":"outer-ss","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":1081,"cipher":"aes-128-gcm","password":"accepted-material"}}),
        ]);
        config["outbound_groups"] = serde_json::json!([{"tag":"outer-chain","type":"relay","proxies":["outer-socks","outer-ss"]}]);
        let config = Arc::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap());
        Self::with_config(config)
    }
    fn with_config(config: Arc<zero_config::RuntimeConfig>) -> Self {
        let network = Arc::new(crate::component::background::BackgroundNetwork::new(
            zero_platform_tokio::EgressInterfaceControl::default(),
        ));
        let (registry, factories, devices) =
            crate::register::compiled_registries_with_background(Some(network.clone()));
        let registry = registry;
        devices
            .wireguard
            .as_ref()
            .unwrap()
            .set_packet_return_totals(devices.packet_return_totals.clone());
        let originals = Arc::new(zero_runtime::HostOriginalBudgets::with_packet_root(
            devices.packet_return_totals.clone(),
        ));
        let host = crate::runtime::RuntimeHost::with_original_budgets(factories, originals);
        Self {
            config,
            network,
            registry,
            devices,
            host,
        }
    }
    fn prepare(&self) -> Arc<crate::component::catalog::ComponentPlan> {
        self.host
            .components()
            .prepare_with_device_resources(self.config.clone(), |resources| {
                let deferred = self.devices.prepare_component_resources(
                    &self.config,
                    resources,
                    &self.registry,
                )?;
                resources.set_source_dependencies(
                    self.devices
                        .source_component_dependencies(&self.config, resources)?,
                )?;
                Ok(deferred)
            })
            .unwrap()
    }
    fn materials(&self) -> Arc<[Option<crate::protocol_registry::ColdPacketPathProjection>]> {
        self.devices
            .prepared_outer_materials(&self.config)
            .unwrap()
            .remove("outer-chain")
            .unwrap()
    }
}
#[tokio::test]
async fn actual_unbuilt_source_refresh_reuses_exact_formatter_family_and_accepted_recipe() {
    let actual = Fixture::new();
    let first = actual.prepare();
    let before = actual.materials();
    let codec = before[1]
        .as_ref()
        .unwrap()
        .formatter
        .as_ref()
        .unwrap()
        .codec
        .clone();
    let roots = actual.host.original_budgets();
    assert_eq!(roots.packet.native().occupied.frames, 0);
    assert_eq!(roots.carrier.snapshot().charged, 0);
    let mut pending = actual
        .host
        .components()
        .prepare_resource_start(actual.config.clone(), deadline())
        .await
        .unwrap();
    let materials = actual
        .devices
        .release_component_resources_for_refresh(&actual.config)
        .unwrap();
    actual.network.egress.invalidate_network();
    let refreshed = pending
        .refresh_device_resources(deadline(), |resources| {
            let deferred = actual.devices.prepare_component_resources_with_materials(
                &actual.config,
                resources,
                &actual.registry,
                materials,
            )?;
            resources.set_source_dependencies(
                actual
                    .devices
                    .source_component_dependencies(&actual.config, resources)?,
            )?;
            Ok(deferred)
        })
        .await
        .unwrap();
    let after = actual.materials();
    assert!(
        Arc::ptr_eq(&before, &after),
        "same actual cold source value, not a second pure claim/new association"
    );
    assert!(Arc::ptr_eq(
        &codec,
        &after[1].as_ref().unwrap().formatter.as_ref().unwrap().codec
    ));
    let old = first.configured("wg-out").unwrap();
    let new = refreshed.configured("wg-out").unwrap();
    assert_eq!(old.instance_id().component, new.instance_id().component);
    assert!(new.instance_id().generation > old.instance_id().generation);
    assert_eq!(roots.packet.native().occupied.frames, 0);
    assert_eq!(
        roots.carrier.snapshot().charged,
        0,
        "no native/task original is fabricated by cold refresh"
    );
    drop(pending);
    actual.host.components().shutdown(deadline()).await.unwrap();
    actual
        .devices
        .release_component_resources(&actual.config)
        .unwrap();
}

#[cfg(feature = "vless")]
#[tokio::test]
async fn wireguard_stream_donors_are_prepared_before_their_outer_consumers_in_one_cold_host() {
    for linked in [false, true] {
        for proxied in [false, true] {
            let mut config = serde_json::to_value(configuration()).unwrap();
            let mut donor = config["outbounds"][0].clone();
            donor["tag"] = "wg-donor".into();
            if linked {
                let mut input = config["inbounds"][0].clone();
                input["tag"] = "donor-in".into();
                input["listen"]["port"] = 51830.into();
                config["inbounds"].as_array_mut().unwrap().push(input);
                donor["protocol"]["inbound_tag"] = "donor-in".into();
            } else {
                donor["protocol"]
                    .as_object_mut()
                    .unwrap()
                    .remove("inbound_tag");
            }
            if proxied {
                donor["protocol"]["outer_udp_proxy"] = "socks-root".into();
            }
            config["outbounds"][0]["protocol"]["outer_udp_proxy"] = "wg-prefix".into();
            config["outbounds"].as_array_mut().unwrap().extend([
                donor,
                serde_json::json!({"tag":"vless-root","protocol":{"type":"vless","server":"127.0.0.1","port":443,"id":"actual-wg-prefix"}}),
                serde_json::json!({"tag":"socks-root","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}}),
            ]);
            config["outbound_groups"] = serde_json::json!([{"tag":"wg-prefix","type":"relay","proxies":["wg-donor","vless-root"]}]);
            let actual = Fixture::with_config(Arc::new(
                zero_config::RuntimeConfig::parse(&config.to_string()).unwrap(),
            ));
            let plan = actual.prepare();
            let consumer = plan.configured("wg-out").unwrap();
            let donor = plan.configured("wg-donor").unwrap();
            let consumer_root = plan
                .device_component(consumer.recipe.spec().dependencies[0])
                .unwrap();
            assert!(consumer_root
                .recipe
                .spec()
                .dependencies
                .contains(&donor.instance_id().component));
            assert!(donor
                .recipe
                .spec()
                .roles
                .contains(&zero_platform_tokio::contract::component::RoleKind::Stream));
            assert!(
                actual
                    .devices
                    .wireguard
                    .as_ref()
                    .unwrap()
                    .cold_graphs
                    .lock()
                    .unwrap()
                    .is_none(),
                "cold lookup cannot pin the actual graph beyond its original preparation pass"
            );
            let roots = actual.host.original_budgets();
            assert_eq!(roots.carrier.snapshot().charged, 0);
            assert_eq!(roots.packet.native().occupied.frames, 0);
        }
    }
}
