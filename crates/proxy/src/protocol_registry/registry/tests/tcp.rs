//! Actual adapter results, not a second test-only claimer implementation.
use std::sync::Arc;

use super::fixtures::{compiled_in_outbound_leaves, outbound_leaf_name};

#[test]
fn cold_tcp_projections_retain_one_executable_owner_across_clones_and_relay_views() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    for (config, leaf, _) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let claimed = registry.claim_outbound_leaf(&config, leaf);
        if protocol == "hysteria2" {
            assert!(
                claimed.is_err(),
                "HY2 must not restore its retired compatibility projection"
            );
            continue;
        }
        let claimed = claimed.unwrap();
        if !claimed.has_tcp_capability() {
            continue;
        }
        let operation = claimed
            .tcp_connect()
            .unwrap_or_else(|failure| panic!("{protocol}: {}", failure.error));
        let retained = claimed;
        let cloned = retained
            .tcp_connect()
            .unwrap_or_else(|failure| panic!("{protocol}: {}", failure.error));
        assert!(Arc::ptr_eq(&operation, &cloned), "{protocol}");
        if matches!(
            protocol,
            "vless" | "vmess" | "trojan" | "socks5" | "shadowsocks" | "mieru"
        ) {
            let (_, _, relay) = retained.tcp_relay_hop().unwrap();
            assert_eq!(
                Arc::as_ptr(&operation) as *const (),
                Arc::as_ptr(&relay) as *const (),
                "{protocol}"
            );
        } else {
            assert!(retained.tcp_relay_hop().is_err(), "{protocol}");
        }
    }
}

#[cfg(feature = "udp-runtime")]
#[test]
fn live_direct_policy_validation_is_deferred_until_the_selected_plane() {
    let registry = crate::register::protocol_registry();
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let leaf = zero_engine::ResolvedLeafOutbound::Direct {
        tag: None,
        dial_policy: zero_traits::DialPolicy {
            interface: Some("zero-interface-that-does-not-exist".into()),
            ..Default::default()
        },
        dial_generation: 0,
    };
    let claimed = registry.claim_outbound_leaf(&config, leaf).unwrap();
    assert!(claimed.has_udp_flow_capability());
    for _ in 0..2 {
        let leaf = &claimed;
        let failure = leaf
            .tcp_connect()
            .err()
            .expect("validate current interface before TCP I/O");
        assert_eq!(failure.stage, "validate_dial_policy");
        assert!(failure.upstream_endpoint.is_none());
        assert!(failure.network.is_none());
    }
    let udp = claimed
        .into_udp_flow()
        .err()
        .expect("UDP independently validates the same current interface");
    assert_eq!(udp.stage, "validate_dial_policy");
}
