//! Exercise real adapter projections, without a second test-only protocol SPI.
use std::{path::PathBuf, sync::Arc};

use super::fixtures::{compiled_in_outbound_leaves, outbound_leaf_name};

#[test]
fn packet_path_operations_are_cold_and_shared_across_leaf_clones() {
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
        if protocol != "vless" {
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

#[cfg(feature = "shadowsocks")]
#[test]
fn cold_ss_formatter_material_is_inert_and_keeps_one_actual_source_clone_family() {
    use zero_core::Address;
    use zero_engine::{OutboundIdentity, ResolvedLeafOutbound};
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    for mode in [
        None,
        Some("tcp_only"),
        Some("udp_only"),
        Some("tcp_and_udp"),
    ] {
        let mut protocol = serde_json::json!({"type":"shadowsocks","server":"same.immutable.endpoint","port":8443,"cipher":"aes-128-gcm","password":"original cold profile"});
        if let Some(mode) = mode {
            protocol["plugin"] = serde_json::json!({"command":"this-program-must-not-be-started-during-cold-formatting","mode":mode});
        }
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({"outbounds":[{"tag":"cold","protocol":protocol}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let leaf = || ResolvedLeafOutbound::Proxy {
            identity: OutboundIdentity::from_config_index(0),
        };
        let material = registry
            .prepare_cold_packet_path_projection(&config, leaf())
            .unwrap();
        if matches!(mode, Some("udp_only" | "tcp_and_udp")) {
            assert!(
                material.is_none(),
                "SIP003u keeps its existing associated/native producer, not a new Codec suffix"
            );
            continue;
        }
        let material = material.expect("original plain UDP carrier including TCP-only plugin");
        assert!(
            !material.associated,
            "SS has only its original Codec suffix semantics"
        );
        assert_eq!(material.descriptor.server, "same.immutable.endpoint");
        assert_eq!(material.descriptor.port, 8443);
        let retained_material = material.clone();
        let source = material.formatter.expect("original pure formatter");
        assert!(Arc::ptr_eq(
            &source.codec,
            &retained_material.formatter.unwrap().codec
        ));
        let retained = source.clone();
        assert!(Arc::ptr_eq(&source.codec, &retained.codec));
        assert_eq!(source.descriptor.server, "same.immutable.endpoint");
        assert_eq!(source.descriptor.port, 8443);
        let sizes = source
            .codec
            .encode_buffer_lengths(&Address::Ipv4([127, 0, 0, 1]), 53, 0)
            .unwrap();
        assert_eq!(sizes.len(), 2);
        assert_eq!(
            source.codec.decode_buffer_lengths(65536).unwrap(),
            vec![65536, 255]
        );
        let independent = registry
            .prepare_cold_datagram_source(&config, leaf())
            .unwrap()
            .unwrap();
        assert!(
            !Arc::ptr_eq(&source.codec, &independent.codec),
            "separate actual prepared paths must not merge associations by tag"
        );
    }
}

#[cfg(feature = "vless")]
#[test]
fn nonformatter_metadata_returns_none_before_any_old_projection_or_file_material() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry().without_outbound_projection("vless");
    for (config, leaf, _) in compiled_in_outbound_leaves() {
        if outbound_leaf_name(&config, &leaf) != "vless" {
            continue;
        }
        // Calling the removed projection would be a real error. The cold
        // metadata path must return None before invoking that execution owner.
        assert!(registry
            .prepare_cold_datagram_source(&config, leaf.clone())
            .unwrap()
            .is_none());
        assert!(registry
            .prepare_cold_packet_path_projection(&config, leaf)
            .unwrap()
            .is_none());
    }
}

#[cfg(feature = "socks5")]
#[test]
fn cold_socks_source_uses_only_original_base_metadata_without_an_executor_or_associated_suffix() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let registry = crate::register::protocol_registry();
    let mut actual = 0;
    for (config, leaf, _) in compiled_in_outbound_leaves() {
        if outbound_leaf_name(&config, &leaf) != "socks5" {
            continue;
        }
        let material = registry
            .prepare_cold_packet_path_projection(&config, leaf)
            .unwrap()
            .unwrap();
        assert_eq!(
            material.descriptor.tag.as_deref(),
            Some(config.outbounds[0].tag())
        );
        let (server, port) = config.outbounds[0].protocol.endpoint().unwrap();
        assert_eq!(
            (&*material.descriptor.server, material.descriptor.port),
            (server, port)
        );
        assert!(!material.descriptor.cache_key.is_empty());
        assert!(!material.associated);
        assert!(material.formatter.is_none());
        actual += 1;
    }
    assert_eq!(actual, 1, "exercise the real compiled SOCKS projection");
}
