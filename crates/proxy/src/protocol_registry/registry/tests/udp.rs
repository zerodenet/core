//! Real, source-materialized UDP results transferred once into execution.
use super::fixtures::{compiled_in_outbound_leaves, outbound_leaf_name};

#[test]
fn cold_udp_projection_moves_existing_operations_without_a_runtime_or_second_preparation() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    for (config, leaf, _) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let claimed = registry.claim_outbound_leaf(&config, leaf);
        if matches!(
            protocol,
            "hysteria2"
                | "vless"
                | "vmess"
                | "trojan"
                | "mieru"
                | "socks5"
                | "shadowsocks"
                | "wireguard"
                | "direct"
        ) {
            assert!(
                claimed.is_err(),
                "migrated protocol must not restore its retired compatibility projection"
            );
            continue;
        }
        let claimed = claimed.unwrap();
        let has_udp = claimed.has_udp_flow_capability();
        let expected = matches!(protocol, "shadowsocks");
        assert_eq!(has_udp, expected, "{protocol}");
        let operation = claimed.into_udp_flow();
        assert_eq!(operation.is_ok(), expected, "{protocol}");
    }
}
