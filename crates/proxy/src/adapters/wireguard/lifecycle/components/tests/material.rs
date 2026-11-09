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
        let network = Arc::new(crate::component::background::BackgroundNetwork::new(
            zero_platform_tokio::EgressInterfaceControl::default(),
        ));
        let (registry, factories, devices) =
            crate::register::compiled_registries_with_background(Some(network.clone()));
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
            let aliases = actual
                .devices
                .device_alias_overrides(&actual.config, resources)?;
            resources.set_device_alias_overrides(aliases)?;
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
