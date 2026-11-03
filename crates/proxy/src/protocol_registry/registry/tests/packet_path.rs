//! Exercise real adapter projections, without a second test-only protocol SPI.
use std::{path::PathBuf, sync::Arc};

use super::fixtures::{compiled_in_outbound_leaves, outbound_leaf_name};

#[test]
fn packet_path_operations_are_cold_and_shared_across_leaf_clones() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    for (config, leaf, _) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let claimed = registry.claim_outbound_leaf(&config, leaf).unwrap();
        let Some(operation) = claimed.udp_packet_path() else {
            assert!(!matches!(
                protocol,
                "socks5" | "shadowsocks" | "hysteria2" | "vless"
            ));
            continue;
        };
        let cloned = claimed;
        let retained = cloned.udp_packet_path().unwrap();
        assert!(Arc::ptr_eq(&operation, &retained), "{protocol}");
        let descriptor = retained.carrier_descriptor().unwrap();
        assert_eq!(descriptor.tag.as_deref(), Some(config.outbounds[0].tag()));
        assert!(!descriptor.cache_key.is_empty());
    }
}

#[test]
fn packet_path_source_identity_is_fixed_at_the_actual_config_boundary() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    for (mut config, leaf, _) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        if !matches!(protocol, "vless" | "hysteria2") {
            continue;
        }
        // No directory or CA I/O is needed to materialize the source-aware
        // recipe; the same captured source must reach later execution.
        config.source_dir = Some(PathBuf::from("/nonexistent/zero-source-a"));
        let first = registry.claim_outbound_leaf(&config, leaf.clone()).unwrap();
        let operation = first.udp_packet_path().unwrap();
        let key = operation.carrier_descriptor().unwrap().cache_key;
        drop(first);
        let mut changed = config.clone();
        changed.source_dir = Some(PathBuf::from("/nonexistent/zero-source-b"));
        let next = registry.claim_outbound_leaf(&changed, leaf).unwrap();
        let next = next.udp_packet_path().unwrap();
        assert_ne!(
            key,
            next.carrier_descriptor().unwrap().cache_key,
            "{protocol}"
        );
        assert_eq!(key, operation.carrier_descriptor().unwrap().cache_key);
    }
}
