use super::*;

fn config(relay: &[&str], prefix: serde_json::Value) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[
            {"tag":"prefix","protocol":prefix},
            {"tag":"pool","protocol":{"type":"mieru","server":"127.0.0.1","port":1234,"password":"p"}},
            {"tag":"other-pool","protocol":{"type":"mieru","server":"127.0.0.1","port":1235,"password":"p"}}
        ],
        "outbound_groups":[{"tag":"relay","type":"relay","proxies":relay}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}

#[test]
#[cfg(feature = "vless")]
fn mixed_legacy_prefix_never_promotes_half_of_the_one_mieru_pool() {
    let config = config(
        &["prefix", "pool"],
        serde_json::json!({
            "type":"vless", "server":"127.0.0.1", "port":443,
            "id":"11111111-2222-3333-4444-555555555555",
            "http_upgrade":{"path":"/transport"}
        }),
    );
    assert!(!production_selected(Some(&config)));
    let proxy = crate::Proxy::new(config).unwrap();
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    for tag in ["pool", "other-pool"] {
        assert!(
            plan.configured(tag).is_none(),
            "the same pool must retain one compatibility owner"
        );
    }
}

#[test]
fn mixed_legacy_suffix_cannot_force_an_already_native_pool_back_to_compatibility() {
    let config = config(
        &["pool", "prefix"],
        serde_json::json!({
            "type":"vless", "server":"127.0.0.1", "port":443,
            "id":"11111111-2222-3333-4444-555555555555",
            "http_upgrade":{"path":"/transport"}
        }),
    );
    assert!(!production_selected(Some(&config)));
}

#[test]
fn unrelated_unmigrated_node_does_not_disable_the_actual_mieru_graph() {
    let config = config(
        &["pool", "other-pool"],
        serde_json::json!({
            "type":"vless", "server":"127.0.0.1", "port":443,
            "id":"11111111-2222-3333-4444-555555555555",
            "http_upgrade":{"path":"/transport"}
        }),
    );
    assert!(production_selected(Some(&config)));
}

#[cfg(feature = "vmess")]
#[test]
fn datagram_only_grpc_role_cannot_qualify_a_stream_prefix() {
    let config = config(
        &["prefix", "pool"],
        serde_json::json!({
            "type":"vmess", "server":"127.0.0.1", "port":443,
            "id":"11111111-2222-3333-4444-555555555555", "grpc":{"service_names":["transport"]}
        }),
    );
    assert!(!production_selected(Some(&config)));
}

#[test]
fn unrelated_builtin_relay_does_not_change_pool_selection() {
    let mut config = config(
        &["pool", "other-pool"],
        serde_json::json!({
            "type":"socks5", "server":"127.0.0.1", "port":443
        }),
    );
    config
        .outbound_groups
        .push(zero_config::OutboundGroupConfig {
            tag: "unrelated".into(),
            group: zero_config::OutboundGroupKind::Relay {
                proxies: vec!["direct".into()],
            },
        });
    assert!(production_selected(Some(&config)));
}

fn packet_pool(config: &mut RuntimeConfig) {
    let OutboundProtocolConfig::Mieru { transport, .. } = &mut config.outbounds[1].protocol else {
        unreachable!()
    };
    *transport = zero_config::MieruTransport::Udp;
}

#[test]
fn root_socks_packet_donor_qualifies_every_profile_in_the_same_original_pool() {
    let mut config = config(
        &["prefix", "pool"],
        serde_json::json!({"type":"socks5", "server":"127.0.0.1", "port":443}),
    );
    packet_pool(&mut config);
    assert!(production_selected(Some(&config)));
    let proxy = crate::Proxy::new(config).unwrap();
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    assert!(plan.configured("prefix").is_some());
    for tag in ["pool", "other-pool"] {
        assert!(
            plan.configured(tag).is_some(),
            "all profiles share the actual native pool after the root donor acquired real dual-original custody"
        );
    }
}

#[test]
fn root_packet_pool_and_real_mieru_packet_donor_keep_existing_native_qualification() {
    for relay in [vec!["pool", "other-pool"], vec!["other-pool", "pool"]] {
        let mut config = config(
            &relay,
            serde_json::json!({"type":"socks5", "server":"127.0.0.1", "port":443}),
        );
        packet_pool(&mut config);
        assert!(production_selected(Some(&config)));
    }
}

#[test]
fn every_root_packet_prefix_branch_has_a_real_native_donor() {
    let mut config = config(
        &["prefix", "pool"],
        serde_json::json!({"type":"socks5", "server":"127.0.0.1", "port":443}),
    );
    packet_pool(&mut config);
    let OutboundGroupKind::Relay { proxies } = &mut config.outbound_groups[0].group else {
        unreachable!()
    };
    proxies[0] = "choice".into();
    config
        .outbound_groups
        .push(zero_config::OutboundGroupConfig {
            tag: "choice".into(),
            group: OutboundGroupKind::Selector {
                outbounds: vec!["other-pool".into(), "prefix".into()],
                default: Some("other-pool".into()),
                selected: Some("other-pool".into()),
            },
        });
    assert!(production_selected(Some(&config)));
    let choice = config.outbound_groups.last_mut().unwrap();
    let OutboundGroupKind::Selector { outbounds, .. } = &mut choice.group else {
        unreachable!()
    };
    outbounds.reverse();
    assert!(production_selected(Some(&config)));
}

#[test]
fn unqualified_udp_suffix_keeps_all_mieru_profiles_on_the_existing_executor() {
    let config = config(
        &["pool", "prefix"],
        serde_json::json!({"type":"socks5", "server":"127.0.0.1", "port":443}),
    );
    assert!(!production_selected(Some(&config)));
    let proxy = crate::Proxy::new(config).unwrap();
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    assert!(plan.configured("prefix").is_some());
    for tag in ["pool", "other-pool"] {
        assert!(plan.configured(tag).is_none());
    }
}

#[test]
fn unselected_udp_suffix_branch_cannot_make_a_native_mieru_prefix_fall_back() {
    let mut config = config(
        &["pool", "other-pool"],
        serde_json::json!({"type":"socks5", "server":"127.0.0.1", "port":443}),
    );
    let OutboundGroupKind::Relay { proxies } = &mut config.outbound_groups[0].group else {
        unreachable!()
    };
    proxies[1] = "choice".into();
    config
        .outbound_groups
        .push(zero_config::OutboundGroupConfig {
            tag: "choice".into(),
            group: OutboundGroupKind::Selector {
                outbounds: vec!["other-pool".into(), "prefix".into()],
                default: Some("other-pool".into()),
                selected: Some("other-pool".into()),
            },
        });
    assert!(!production_selected(Some(&config)));
    let OutboundGroupKind::Selector { outbounds, .. } =
        &mut config.outbound_groups.last_mut().unwrap().group
    else {
        unreachable!()
    };
    outbounds.retain(|tag| tag == "other-pool");
    assert!(production_selected(Some(&config)));
}

#[test]
fn an_unselected_nonroot_socks_packet_branch_still_does_not_invent_a_new_udp_capability() {
    let mut config = config(
        &["prefix", "pool"],
        serde_json::json!({"type":"socks5", "server":"127.0.0.1", "port":443}),
    );
    packet_pool(&mut config);
    let OutboundGroupKind::Relay { proxies } = &mut config.outbound_groups[0].group else {
        unreachable!()
    };
    proxies[0] = "choice".into();
    config
        .outbound_groups
        .push(zero_config::OutboundGroupConfig {
            tag: "nonroot".into(),
            group: OutboundGroupKind::Relay {
                proxies: vec!["other-pool".into(), "prefix".into()],
            },
        });
    config
        .outbound_groups
        .push(zero_config::OutboundGroupConfig {
            tag: "choice".into(),
            group: OutboundGroupKind::Selector {
                outbounds: vec!["other-pool".into(), "nonroot".into()],
                default: Some("other-pool".into()),
                selected: Some("other-pool".into()),
            },
        });
    assert!(!production_selected(Some(&config)));
    let OutboundGroupKind::Selector { outbounds, .. } =
        &mut config.outbound_groups.last_mut().unwrap().group
    else {
        unreachable!()
    };
    outbounds.reverse();
    assert!(!production_selected(Some(&config)));
}

#[test]
fn pending_original_metadata_uses_actual_declarations_and_deduplicates_the_one_shared_pool() {
    let mut config = config(
        &["prefix", "pool"],
        serde_json::json!({"type":"socks5", "server":"127.0.0.1", "port":443}),
    );
    packet_pool(&mut config);
    config.runtime.native_stream_max_originals = Some(2);
    config.runtime.mieru_max_carriers = Some(1);
    let proxy = crate::Proxy::new(config).unwrap();
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let socks = plan.configured("prefix").unwrap();
    let pool = plan.configured("pool").unwrap();
    let other = plan.configured("other-pool").unwrap();
    let original_capacity = plan
        .pending_carrier_capacity(&[socks.role_id(), pool.role_id()])
        .unwrap();
    assert!(
        original_capacity >= 3,
        "two actual SOCKS originals and the receiver cannot be counted as only two roles"
    );
    assert_eq!(
        original_capacity,
        plan.pending_carrier_capacity(&[socks.role_id(), pool.role_id(), other.role_id()])
            .unwrap(),
        "another profile of this same actual pool cannot double its declaration budget",
    );
    assert!(
        socks.original_carrier_owner(0).is_none(),
        "cold metadata projection cannot register or open a native original"
    );
}
