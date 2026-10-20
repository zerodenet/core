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
fn static_device_recipe_keeps_actual_first_root_binding_and_identity_together_without_runtime() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let inventory = ProtocolInventory::default();
    let config = static_config(&["a", "b"]);
    let egress = EgressInterfaceControl::default();
    let generation = NetworkGeneration(egress.generation());
    let paths = inventory
        .prepare_device_packet_paths(
            &config,
            &zero_engine::EndpointAdmission::from_config(&config),
            generation,
        )
        .unwrap();
    let single = paths.get("a").expect("actual claimed single carrier");
    assert_eq!(
        single.root_binding,
        root_binding(&config.outbounds[0], generation)
    );
    assert_eq!(single.identity, path_identity(&config, &[0]).unwrap());
    assert!(single.operation.carrier_descriptor().is_some());
    let chain = paths.get("chain").expect("actual prepared datagram relay");
    assert_eq!(
        chain.root_binding,
        root_binding(&config.outbounds[0], generation)
    );
    assert_eq!(chain.identity, path_identity(&config, &[0, 1]).unwrap());
    assert!(chain.root_binding.prefix.is_empty()); // physical root has no prior role.
    let reversed = static_config(&["b", "a"]);
    let reversed_paths = inventory
        .prepare_device_packet_paths(
            &reversed,
            &zero_engine::EndpointAdmission::from_config(&reversed),
            generation,
        )
        .unwrap();
    assert_eq!(
        reversed_paths["chain"].root_binding,
        root_binding(&reversed.outbounds[1], generation)
    );
    assert_ne!(chain.identity, reversed_paths["chain"].identity);
}
#[test]
fn physical_root_projection_preserves_every_actual_direct_dial_requirement() {
    let config = RuntimeConfig::parse(r#"{"outbounds":[{"tag":"bound","protocol":{"type":"direct"},"dial":{"address_family":"only_ipv4","interface":"fixture-interface","source_ip":"127.0.0.1"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
    let egress = EgressInterfaceControl::default();
    let generation = NetworkGeneration(egress.generation());
    let binding = root_binding(&config.outbounds[0], generation);
    assert_eq!(binding.dial, config.outbounds[0].dial.to_policy());
    assert_eq!(
        binding.dial.address_family,
        zero_traits::AddressFamily::OnlyIpv4
    );
    assert_eq!(binding.dial.interface.as_deref(), Some("fixture-interface"));
    assert_eq!(binding.dial.source_ip, Some("127.0.0.1".parse().unwrap()));
    assert_eq!(binding.network_generation.0, egress.generation());
    assert!(binding.prefix.is_empty());
}
