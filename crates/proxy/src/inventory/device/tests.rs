#![cfg(feature = "shadowsocks")]
use super::*;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::EgressInterfaceControl;

fn static_config(proxies: &[&str]) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[
            {"tag":"a", "protocol":{"type":"shadowsocks", "server":"127.0.0.1", "port":10101, "cipher":"chacha20-ietf-poly1305", "password":"projection-fixture-a"}},
            {"tag":"b", "protocol":{"type":"shadowsocks", "server":"127.0.0.1", "port":10102, "cipher":"chacha20-ietf-poly1305", "password":"projection-fixture-b"}}
        ],
        "outbound_groups":[{"tag":"chain", "type":"relay", "proxies":proxies}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
#[test]
fn unused_outbounds_and_relay_aliases_never_create_claimed_device_paths_without_a_runtime() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let inventory = ProtocolInventory::default();
    let config = static_config(&["a", "b"]);
    let paths = inventory
        .prepare_device_packet_paths(
            &config,
            &zero_engine::EndpointAdmission::from_config(&config),
            NetworkGeneration(EgressInterfaceControl::default().generation()),
            None,
        )
        .unwrap();
    assert!(
        paths.is_empty(),
        "no actual device requested any of these carriers"
    );
}

#[cfg(feature = "wireguard")]
fn with_device(proxies: &[&str], target: &str) -> Arc<RuntimeConfig> {
    let mut config = serde_json::to_value(static_config(proxies)).unwrap();
    config["outbounds"].as_array_mut().unwrap().push(serde_json::json!({
        "tag":"wg", "protocol":{"type":"wireguard", "private_key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
            "addresses":["10.0.0.2/32"], "outer_udp_proxy":target,
            "peers":[{"public_key":"AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=", "endpoint":"127.0.0.1:51820", "allowed_ips":["0.0.0.0/0"]}]}
    }));
    Arc::new(RuntimeConfig::parse(&config.to_string()).unwrap())
}

#[cfg(feature = "wireguard")]
fn prepare(
    config: &Arc<RuntimeConfig>,
) -> (
    ProtocolInventory,
    Arc<crate::component::catalog::ComponentPlan>,
    EgressInterfaceControl,
) {
    let egress = EgressInterfaceControl::default();
    let network = Arc::new(crate::component::background::BackgroundNetwork::new(
        egress.clone(),
    ));
    let mut inventory = ProtocolInventory::with_background(network);
    inventory.registry = Arc::new(
        inventory
            .registry
            .as_ref()
            .clone()
            .without_outbound_projection("shadowsocks"),
    );
    inventory
        .devices
        .wireguard
        .as_ref()
        .unwrap()
        .set_packet_return_totals(inventory.devices.packet_return_totals.clone());
    let plan = inventory.prepare_device_components(config.clone()).unwrap();
    (inventory, plan, egress)
}

#[cfg(feature = "wireguard")]
#[tokio::test]
async fn device_recipe_consumes_only_its_same_candidate_standard_material_and_actual_root() {
    let mut identities = Vec::new();
    for proxies in [["a", "b"], ["b", "a"]] {
        let config = with_device(&proxies, "chain");
        let (inventory, plan, egress) = prepare(&config);
        let generation = NetworkGeneration(egress.generation());
        let paths = inventory
            .prepare_device_packet_paths(
                &config,
                &zero_engine::EndpointAdmission::from_config(&config),
                generation,
                Some(&plan),
            )
            .unwrap();
        assert_eq!(
            paths.len(),
            1,
            "only the referenced path, not unused leaves"
        );
        let chain = &paths["chain"];
        let first = plan.configured(proxies[0]).unwrap();
        assert_eq!(chain.recipe.binding().dial, first.dial);
        assert_eq!(chain.recipe.binding().network_generation, generation);
        assert!(chain.recipe.binding().prefix.is_empty());
        assert!(first
            .recipe
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Datagram));
        let indices = source::indices(&config, "chain").unwrap();
        assert_eq!(chain.identity, path_identity(&config, &indices).unwrap());
        identities.push(chain.identity);
        let roots = inventory.runtime_host().original_budgets();
        assert_eq!(
            roots.carrier.snapshot().charged,
            0,
            "cold source creates no native carrier"
        );
        assert_eq!(roots.packet.native().occupied.frames, 0);
    }
    assert_ne!(identities[0], identities[1]);
}

#[cfg(feature = "wireguard")]
#[tokio::test]
async fn a_single_member_relay_and_leaf_keep_the_exact_actual_dial_policy_without_legacy_preparation(
) {
    for target in ["a", "chain"] {
        let mut value = serde_json::to_value(with_device(&["a"], target).as_ref()).unwrap();
        value["outbounds"][0]["dial"] = serde_json::json!({
            "address_family":"only_ipv4", "interface":"fixture-interface", "source_ip":"127.0.0.1"
        });
        let config = Arc::new(RuntimeConfig::parse(&value.to_string()).unwrap());
        let (inventory, plan, egress) = prepare(&config);
        let paths = inventory
            .prepare_device_packet_paths(
                &config,
                &zero_engine::EndpointAdmission::from_config(&config),
                NetworkGeneration(egress.generation()),
                Some(&plan),
            )
            .unwrap();
        assert_eq!(paths.len(), 1);
        let path = &paths[target];
        assert_eq!(
            path.recipe.binding().dial,
            config.outbounds[0].dial.to_policy()
        );
        assert!(path.recipe.binding().prefix.is_empty());
        assert_eq!(path.identity, path_identity(&config, &[0]).unwrap());
    }
}

#[cfg(feature = "wireguard")]
#[test]
fn missing_actual_source_plan_is_an_error_instead_of_rebuilding_compatibility_operations() {
    let config = with_device(&["a", "b"], "chain");
    let inventory = ProtocolInventory::default();
    let Err(failure) = inventory.prepare_device_packet_paths(
        &config,
        &zero_engine::EndpointAdmission::from_config(&config),
        NetworkGeneration(1),
        None,
    ) else {
        panic!("missing actual Source plan was replaced by a compatibility path");
    };
    assert!(failure.to_string().contains("actual candidate plan"));
}
