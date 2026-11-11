//! Pure consumer facts; these tests grant no native or role admission.
use super::*;
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
#[test]
fn relay_consumers_include_unselected_nested_aliases_and_do_not_require_protocol_projection() {
    let mut config = config();
    assert!(used_by_relay(&config, "used"));
    assert!(used_by_relay(&config, "unused"));
    assert!(used_by_relay(&config, "sink"));
    assert!(!used_by_relay(&config, "absent"));
    config.outbound_groups.retain(|group| group.tag != "chain");
    assert!(!used_by_relay(&config, "used"));
}
#[test]
fn wireguard_outer_consumer_is_separate_from_ordinary_relay_consumers() {
    let mut config = config();
    config.outbound_groups.retain(|group| group.tag != "chain");
    let node = serde_json::from_value(serde_json::json!({
        "tag":"wg","protocol":{"type":"wireguard","private_key":"01".repeat(32),
        "addresses":["10.0.0.1/32"],"mtu":1420,"outer_udp_proxy":"alias",
        "peers":[{"public_key":"02".repeat(32),"endpoint":"127.0.0.1:51820","allowed_ips":["0.0.0.0/0"]}]}
    })).unwrap();
    config.outbounds.push(node);
    assert!(used_by_outer_udp(&config, "used"));
    assert!(used_by_outer_udp(&config, "unused"));
    assert!(!used_by_outer_udp(&config, "sink"));
    assert!(!used_by_relay(&config, "used"));
    config.outbounds.pop();
    assert!(!used_by_outer_udp(&config, "used"));
}
