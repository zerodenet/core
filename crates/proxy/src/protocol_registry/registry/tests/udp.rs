//! Real, source-materialized UDP results transferred once into execution.
use super::fixtures::{compiled_in_outbound_leaves, outbound_leaf_name};

#[test]
fn cold_udp_projection_moves_existing_operations_without_a_runtime_or_second_preparation() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    for (config, leaf, _) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let claimed = registry.claim_outbound_leaf(&config, leaf).unwrap();
        let has_udp = claimed.has_udp_flow_capability();
        let expected = matches!(
            protocol,
            "direct" | "vless" | "vmess" | "trojan" | "hysteria2" | "shadowsocks" | "mieru"
        );
        assert_eq!(has_udp, expected, "{protocol}");
        let operation = claimed.into_udp_flow();
        if protocol == "vmess" {
            let failure = operation
                .err()
                .expect("ordinary VMess UDP uses the standard Datagram role");
            assert_eq!(failure.stage, "udp_standard_role");
            assert_eq!(failure.upstream, Some(("127.0.0.1".into(), 443)));
        } else {
            assert_eq!(operation.is_ok(), expected, "{protocol}");
        }
    }
}

#[test]
fn cold_udp_relay_results_keep_the_existing_binding_capabilities() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    for (config, leaf, _) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let claimed = registry.claim_outbound_leaf(&config, leaf).unwrap();
        let relay = claimed.into_udp_relay();
        assert_eq!(
            relay.is_ok(),
            matches!(protocol, "vless" | "vmess" | "trojan" | "mieru"),
            "{protocol}"
        );
    }
}
