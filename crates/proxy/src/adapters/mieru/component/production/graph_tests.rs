use super::production_selected;
use zero_config::{MieruTransport, OutboundGroupKind, OutboundProtocolConfig, RuntimeConfig};

fn config(packet_receiver: bool) -> RuntimeConfig {
    let mut config = RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[
            {"tag":"pool","protocol":{"type":"mieru","server":"127.0.0.1","port":1234,"password":"p"}},
            {"tag":"receiver","protocol":{"type":"mieru","server":"127.0.0.1","port":1235,"password":"p"}},
            {"tag":"business","protocol":{"type":"socks5","server":"127.0.0.1","port":1236}}
        ],
        "outbound_groups":[
            {"tag":"prefix","type":"relay","proxies":["pool","business"]},
            {"tag":"choice","type":"selector","outbounds":["receiver","prefix"],"selected":"receiver"},
            {"tag":"whole","type":"relay","proxies":["choice","receiver"]}
        ],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    if packet_receiver {
        let OutboundProtocolConfig::Mieru { transport, .. } = &mut config.outbounds[1].protocol
        else {
            unreachable!()
        };
        *transport = MieruTransport::Udp;
    }
    config
}

#[test]
fn nested_tcp_prefix_uses_one_native_pool_in_every_selector_order() {
    let mut config = config(false);
    assert!(production_selected(Some(&config)));
    let OutboundGroupKind::Selector { outbounds, .. } = &mut config.outbound_groups[1].group else {
        unreachable!()
    };
    outbounds.reverse();
    assert!(production_selected(Some(&config)));
}

#[test]
fn terminal_tcp_support_never_grants_packet_custody_to_an_unselected_udp_underlay() {
    let mut config = config(true);
    assert!(!production_selected(Some(&config)));
    let OutboundGroupKind::Selector { outbounds, .. } = &mut config.outbound_groups[1].group else {
        unreachable!()
    };
    outbounds.reverse();
    assert!(!production_selected(Some(&config)));
}

#[test]
fn removing_only_the_unqualified_udp_consumer_preserves_the_existing_tcp_prefix() {
    let mut config = config(true);
    assert!(!production_selected(Some(&config)));
    config.outbound_groups.pop();
    assert!(production_selected(Some(&config)));
}

#[test]
fn a_packet_pool_can_feed_a_tcp_business_suffix_without_granting_a_new_udp_relay() {
    let mut config = config(false);
    config.outbound_groups.truncate(1);
    let OutboundProtocolConfig::Mieru { transport, .. } = &mut config.outbounds[0].protocol else {
        unreachable!()
    };
    *transport = MieruTransport::Udp;
    assert!(production_selected(Some(&config)));
}
