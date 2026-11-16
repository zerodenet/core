use zero_engine::ResolvedLeafOutbound;

use crate::runtime::path::TcpPathCategory;

use super::fixtures::{compiled_in_outbound_leaves, outbound_leaf_name};

#[test]
fn compiled_in_outbound_leaf_variants_have_expected_protocol_owners() {
    let registry = crate::register::protocol_registry();

    for (config, leaf, expected_owners) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let owner_count = registry
            .entries
            .iter()
            .filter(|entry| entry.support.name() == protocol)
            .count();
        assert_eq!(
            owner_count, expected_owners,
            "{} outbound leaf should have {expected_owners} registered protocol owner(s)",
            protocol
        );

        let claimed = registry.claim_outbound_leaf(&config, leaf.clone());
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
                "migrated protocol must execute its registered standard roles"
            );
            continue;
        }
        assert_eq!(
            claimed
                .as_ref()
                .map(|claim| claim.has_tcp_capability())
                .ok(),
            Some(expected_owners == 1 && !matches!(protocol, "wireguard" | "shadowsocks")),
            "{} claimed outbound lookup should expose runtime facts and optional adapter with the same ownership policy",
            protocol
        );
    }
}

#[test]
fn block_outbound_leaf_is_kernel_fact_not_adapter_protocol() {
    let registry = crate::register::protocol_registry();
    let config = zero_config::RuntimeConfig::parse(
        r#"{ "route": { "rules": [], "final": { "type": "direct" } } }"#,
    )
    .expect("minimal config");
    let leaf = ResolvedLeafOutbound::Block {
        tag: Some("blocked"),
    };

    let owner_count = registry
        .entries
        .iter()
        .filter(|entry| entry.support.name() == "block")
        .count();
    assert_eq!(
        owner_count, 0,
        "block should not have a registered protocol owner"
    );

    let claimed = registry
        .claim_outbound_leaf(&config, leaf.clone())
        .expect("block should still expose claimed runtime facts");
    assert!(
        !claimed.has_tcp_capability(),
        "block should not expose an outbound adapter"
    );

    assert_eq!(claimed.runtime.tcp_path, TcpPathCategory::Block);
    #[cfg(feature = "udp-runtime")]
    assert_eq!(claimed.runtime.health_tag, None);
    assert_eq!(claimed.runtime.endpoint, None);
    assert_eq!(claimed.runtime.kernel_tag, Some("blocked".to_owned()));
}

#[cfg(feature = "udp-runtime")]
#[test]
fn udp_compatibility_claims_exclude_migrated_datagram_roles() {
    let registry = crate::register::protocol_registry();

    for (config, leaf, expected_claims) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let claimed = registry.claim_outbound_leaf(&config, leaf.clone());
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
                "migrated protocol must execute its registered standard roles"
            );
            continue;
        }
        assert_eq!(
            claimed
                .as_ref()
                .map(|claim| claim.has_udp_flow_capability())
                .ok(),
            Some(expected_claims == 1 && !matches!(protocol, "wireguard" | "socks5" | "direct")),
            "{} legacy udp-flow lookup must exclude migrated standard roles",
            protocol
        );
    }
}

#[cfg(feature = "udp-runtime")]
#[test]
fn packet_path_metadata_does_not_require_an_outbound_executor() {
    let registry = crate::register::protocol_registry();
    for (config, leaf, _) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let projection = registry
            .prepare_cold_packet_path_projection(&config, leaf)
            .unwrap();
        assert_eq!(
            projection.is_some(),
            matches!(protocol, "socks5" | "shadowsocks")
        );
    }
}
