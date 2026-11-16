//! Pure consumer facts; these tests grant no native or role admission.
use super::*;
use zero_config::OutboundGroupKind;
fn config() -> RuntimeConfig {
    RuntimeConfig::parse(
        r#"{
        "outbounds":[
            {"tag":"used","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}},
            {"tag":"sink","protocol":{"type":"socks5","server":"127.0.0.1","port":1081}},
            {"tag":"unused","protocol":{"type":"socks5","server":"127.0.0.1","port":1082}}
        ],
        "outbound_groups":[
            {"tag":"choice","type":"selector","outbounds":["used","unused"],"selected":"unused"},
            {"tag":"alias","type":"fallback","outbounds":["choice"]},
            {"tag":"chain","type":"relay","proxies":["alias","sink"]}
        ],"route":{"final":{"type":"direct"}}
    }"#,
    )
    .unwrap()
}
#[cfg(feature = "vless")]
#[test]
fn pooled_outer_uses_the_existing_flat_path_grammar_without_excluding_every_pool() {
    let mut config = config();
    config.outbound_groups.clear();
    config.outbounds[0].protocol = serde_json::from_value(serde_json::json!({
        "type":"vless", "server":"127.0.0.1", "port":443,
        "id":"11111111-2222-3333-4444-555555555555",
        "mux_concurrency":2, "xudp_concurrency":2
    }))
    .unwrap();
    config.outbounds.push(
        serde_json::from_value(serde_json::json!({
            "tag":"wg", "protocol":{"type":"wireguard", "private_key":"01".repeat(32),
                "addresses":["10.0.0.1/32"], "outer_udp_proxy":"used",
                "peers":[{"public_key":"02".repeat(32), "endpoint":"127.0.0.1:51820",
                    "allowed_ips":["0.0.0.0/0"]}]}
        }))
        .unwrap(),
    );
    assert!(packet_source_selected(&config, &config.outbounds[3]));
    config.outbound_groups = serde_json::from_value(serde_json::json!([
        {"tag":"outer", "type":"relay", "proxies":["sink", "used"]}
    ]))
    .unwrap();
    let zero_config::OutboundProtocolConfig::Wireguard {
        outer_udp_proxy, ..
    } = &mut config.outbounds[3].protocol
    else {
        unreachable!()
    };
    *outer_udp_proxy = Some("outer".into());
    assert!(packet_source_selected(&config, &config.outbounds[3]));
    // SOCKS has a root association, not an independent tail formatter or an
    // Associated Packet receiver. Preserve that actual protocol grammar.
    config.outbound_groups[0].group = OutboundGroupKind::Relay {
        proxies: vec!["used".into(), "sink".into()],
    };
    assert!(!packet_source_selected(&config, &config.outbounds[3]));
}

#[cfg(feature = "vless")]
#[test]
fn browser_fixed_peer_source_does_not_gain_an_associated_prefix_or_disabled_udp() {
    let mut config = config();
    config.outbound_groups.clear();
    config.outbounds[0].protocol = serde_json::from_value(serde_json::json!({
        "type":"vless", "server":"127.0.0.1", "port":443,
        "id":"11111111-2222-3333-4444-555555555555",
        "ws":{"path":"/wire", "browser_dialer":{}}
    }))
    .unwrap();
    config.outbounds.push(
        serde_json::from_value(serde_json::json!({
            "tag":"wg", "protocol":{"type":"wireguard", "private_key":"01".repeat(32),
                "addresses":["10.0.0.1/32"], "outer_udp_proxy":"used",
                "peers":[{"public_key":"02".repeat(32), "endpoint":"127.0.0.1:51820",
                    "allowed_ips":["0.0.0.0/0"]}]}
        }))
        .unwrap(),
    );
    assert!(packet_source_selected(&config, &config.outbounds[3]));
    assert!(packet_source_selected(&config, &config.outbounds[3]));
    config.outbound_groups = serde_json::from_value(serde_json::json!([
        {"tag":"outer", "type":"relay", "proxies":["used"]}
    ]))
    .unwrap();
    let zero_config::OutboundProtocolConfig::Wireguard {
        outer_udp_proxy, ..
    } = &mut config.outbounds[3].protocol
    else {
        unreachable!()
    };
    *outer_udp_proxy = Some("outer".into());
    assert!(packet_source_selected(&config, &config.outbounds[3]));
    config.outbound_groups[0].group = OutboundGroupKind::Relay {
        proxies: vec!["sink".into(), "used".into()],
    };
    assert!(!packet_source_selected(&config, &config.outbounds[3]));
    config.outbound_groups[0].group = OutboundGroupKind::Relay {
        proxies: vec!["used".into()],
    };
    config.outbounds[0].udp.enabled = false;
    assert!(!packet_source_selected(&config, &config.outbounds[3]));
}

#[cfg(feature = "vless")]
#[test]
fn hopping_carrier_keeps_outermost_grammar_and_can_supply_an_existing_pool_prefix() {
    let mut config = config();
    config.outbound_groups.clear();
    config.outbounds[0].protocol = serde_json::from_value(serde_json::json!({
        "type":"vless", "server":"127.0.0.1", "port":443,
        "id":"11111111-2222-3333-4444-555555555555", "quic":{"insecure":true},
        "hysteria":{"auth":"carrier-secret","quic_parameters":{
            "udp_hop":{"ports":[8443,9443],"interval_min_secs":5,"interval_max_secs":5}
        }}
    }))
    .unwrap();
    config.outbounds[1].protocol = serde_json::from_value(serde_json::json!({
        "type":"vless", "server":"127.0.0.1", "port":1443,
        "id":"11111111-2222-3333-4444-555555555555", "mux_concurrency":2,"xudp_concurrency":2
    }))
    .unwrap();
    config.outbound_groups = serde_json::from_value(serde_json::json!([
        {"tag":"outer","type":"relay","proxies":["used","sink"]}
    ]))
    .unwrap();
    config.outbounds.push(serde_json::from_value(serde_json::json!({
        "tag":"wg", "protocol":{"type":"wireguard", "private_key":"01".repeat(32),
        "addresses":["10.0.0.1/32"], "outer_udp_proxy":"outer", "peers":[{
        "public_key":"02".repeat(32), "endpoint":"127.0.0.1:51820", "allowed_ips":["0.0.0.0/0"]}]}
    })).unwrap());
    assert!(packet_source_selected(&config, &config.outbounds[3]));
    config.outbound_groups[0].group = OutboundGroupKind::Relay {
        proxies: vec!["sink".into(), "used".into()],
    };
    assert!(
        !packet_source_selected(&config, &config.outbounds[3]),
        "the existing hopping implementation never accepts a lower datagram carrier"
    );
}

#[cfg(feature = "hysteria2")]
#[test]
fn hy2_outer_selects_actual_hopping_and_fixed_roots_without_an_associated_fallback() {
    let mut config = config();
    config.outbound_groups.clear();
    config.outbounds[0].protocol = serde_json::from_value(serde_json::json!({
        "type":"hysteria2","server":"127.0.0.1","port":443,"password":"same-secret","insecure":true
    }))
    .unwrap();
    config.outbounds.push(serde_json::from_value(serde_json::json!({
        "tag":"wg","protocol":{"type":"wireguard","private_key":"01".repeat(32),
            "addresses":["10.0.0.1/32"],"outer_udp_proxy":"used",
            "peers":[{"public_key":"02".repeat(32),"endpoint":"127.0.0.1:51820","allowed_ips":["0.0.0.0/0"]}]}
    })).unwrap());
    assert!(packet_source_selected(&config, &config.outbounds[3]));
    config.outbound_groups = serde_json::from_value(serde_json::json!([
        {"tag":"outer","type":"relay","proxies":["sink","used"]}
    ]))
    .unwrap();
    let zero_config::OutboundProtocolConfig::Wireguard {
        outer_udp_proxy, ..
    } = &mut config.outbounds[3].protocol
    else {
        unreachable!()
    };
    *outer_udp_proxy = Some("outer".into());
    assert!(
        !packet_source_selected(&config, &config.outbounds[3]),
        "HY2's actual pool still rejects a lower prefix"
    );
    config.outbound_groups[0].group = OutboundGroupKind::Relay {
        proxies: vec!["used".into()],
    };
    assert!(packet_source_selected(&config, &config.outbounds[3]));
    let zero_config::OutboundProtocolConfig::Hysteria2 { transport, .. } =
        &mut config.outbounds[0].protocol
    else {
        unreachable!()
    };
    transport.udp_hop = Some(
        serde_json::from_value(
            serde_json::json!({"ports":[443,8443],"interval_min_secs":5,"interval_max_secs":5}),
        )
        .unwrap(),
    );
    assert!(
        packet_source_selected(&config, &config.outbounds[3]),
        "rotating wire uses the same actual protocol anchor and live receiver"
    );
}
