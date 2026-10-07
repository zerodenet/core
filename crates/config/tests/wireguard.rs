use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};
use zero_core::Address;
use zero_router::RouteAction;

fn key(byte: u8) -> String {
    STANDARD.encode([byte; 32])
}

fn config(protocol: Value) -> String {
    json!({
        "outbounds": [{"tag": "wg", "protocol": protocol}],
        "route": {"rules": [], "final": {"type": "direct"}}
    })
    .to_string()
}

fn valid_protocol() -> Value {
    json!({
        "type": "wireguard",
        "private_key": key(1),
        "addresses": ["172.16.0.2/32", "fd00::2/128"],
        "peers": [{
            "public_key": key(2),
            "endpoint": "peer.example:51820",
            "allowed_ips": ["0.0.0.0/0", "::/0"],
            "keepalive_secs": 25,
            "reserved": [0, 0, 0]
        }]
    })
}

#[test]
fn linked_wireguard_endpoint_requires_one_matching_inbound() {
    let mut outbound = valid_protocol();
    outbound["inbound_tag"] = json!("wg-in");
    let inbound = json!({
        "tag": "wg-in",
        "listen": {"address": "127.0.0.1", "port": 51820},
        "protocol": {
            "type": "wireguard", "private_key": key(1),
            "peers": [{
                "public_key": key(2), "allowed_ips": ["0.0.0.0/0", "::/0"],
                "keepalive_secs": 25, "reserved": [0, 0, 0]
            }]
        }
    });
    let mut input = json!({
        "inbounds": [inbound],
        "outbounds": [{"tag": "wg", "protocol": outbound}],
        "route": {"rules": [], "final": {"type": "direct"}}
    });
    assert!(RuntimeConfig::parse(&input.to_string()).is_ok());

    input["inbounds"][0]["protocol"]["addresses"] =
        input["outbounds"][0]["protocol"]["addresses"].clone();
    assert!(RuntimeConfig::parse(&input.to_string()).is_ok());
    input["inbounds"][0]["protocol"]["addresses"] = json!(["172.16.0.3/32"]);
    assert!(RuntimeConfig::parse(&input.to_string())
        .unwrap_err()
        .to_string()
        .contains("key, addresses, MTU and ordered peers"));
    input["inbounds"][0]["protocol"]
        .as_object_mut()
        .unwrap()
        .remove("addresses");

    input["outbounds"].as_array_mut().unwrap().push(json!({
        "tag": "outer", "protocol": {"type": "socks5", "server": "127.0.0.1", "port": 1080}
    }));
    input["outbounds"][0]["protocol"]["outer_udp_proxy"] = json!("outer");
    assert!(RuntimeConfig::parse(&input.to_string()).is_ok());

    input["inbounds"][0]["protocol"]["peers"][0]["allowed_ips"] = json!(["10.0.0.0/8"]);
    assert!(RuntimeConfig::parse(&input.to_string())
        .unwrap_err()
        .to_string()
        .contains("must use the linked inbound's key, addresses, MTU and ordered peers"));

    input["inbounds"] = json!([]);
    assert!(RuntimeConfig::parse(&input.to_string())
        .unwrap_err()
        .to_string()
        .contains("missing inbound"));
}

fn multi_outbound_config() -> Value {
    json!({
        "outbounds": [
            {"tag":"a", "protocol":{
                "type":"wireguard", "private_key":key(1),
                "addresses":["10.10.0.11/32"],
                "peers":[{"public_key":key(2), "endpoint":"127.0.0.1:51820",
                    "allowed_ips":["10.10.0.0/24", "192.168.0.0/23"]}]
            }},
            {"tag":"b", "protocol":{
                "type":"wireguard", "private_key":key(3),
                "addresses":["10.68.1.1/24"],
                "peers":[{"public_key":key(4), "endpoint":"127.0.0.1:51821",
                    "allowed_ips":["10.0.0.0/8"]}]
            }}
        ],
        "route":{"rules":[], "auto_outbounds":["b", "a"], "final":{"type":"direct"}},
        "runtime":{"dns":{
            "servers":{
                "system":{"type":"system"},
                "a-dns":{"type":"udp", "host":"192.168.1.180", "detour":"a"}
            },
            "default_server":"system",
            "dispatch":[{"condition":{"type":"domain", "values":["office.example"]},
                "server":"a-dns"}],
            "policy":{"node_server":"system", "fallback_servers":[]}
        }}
    })
}

fn ip(value: [u8; 4]) -> Address {
    Address::Ipv4(value)
}

#[test]
fn auto_outbounds_use_longest_prefix_across_wireguard_profiles() {
    let config = RuntimeConfig::parse(&multi_outbound_config().to_string()).unwrap();
    let router = config.compile_route().unwrap();
    assert_eq!(
        router.decide(&ip([10, 10, 0, 8]), None),
        RouteAction::Route("a".into())
    );
    assert_eq!(
        router.decide(&ip([10, 68, 1, 8]), None),
        RouteAction::Route("b".into())
    );
    assert_eq!(
        router.decide(&ip([192, 168, 1, 180]), None),
        RouteAction::Route("a".into())
    );
    assert_eq!(router.decide(&ip([8, 8, 8, 8]), None), RouteAction::Direct);

    let dns = config.compile_dns_dispatch().unwrap().unwrap();
    assert_eq!(dns.select("host.office.example"), "a-dns");
    assert_eq!(dns.select("public.example"), "system");
    assert!(!config
        .runtime
        .dns
        .as_ref()
        .unwrap()
        .servers
        .contains_key("b-dns"));
}

#[test]
fn explicit_rules_precede_auto_outbounds() {
    let mut input = multi_outbound_config();
    input["route"]["rules"] = json!([{
        "condition":{"type":"ip", "values":["10.10.0.7/32"]},
        "action":{"type":"direct"}
    }]);
    let config = RuntimeConfig::parse(&input.to_string()).unwrap();
    let router = config.compile_route().unwrap();
    assert_eq!(
        router.decide(&ip([10, 10, 0, 7]), None),
        RouteAction::Direct
    );
    assert_eq!(
        router.decide(&ip([10, 10, 0, 8]), None),
        RouteAction::Route("a".into())
    );
}

#[test]
fn auto_outbounds_reject_ambiguous_or_unsupported_advertisements() {
    let mut input = multi_outbound_config();
    input["outbounds"][1]["protocol"]["peers"][0]["allowed_ips"] = json!(["10.10.0.5/24"]);
    let error = RuntimeConfig::parse(&input.to_string())
        .unwrap_err()
        .to_string();
    assert!(error.contains("automatic IP route `10.10.0.0/24`"));

    let mut input = multi_outbound_config();
    input["route"]["auto_outbounds"] = json!(["missing"]);
    assert!(RuntimeConfig::parse(&input.to_string())
        .unwrap_err()
        .to_string()
        .contains("missing"));

    let mut input = multi_outbound_config();
    input["outbounds"]
        .as_array_mut()
        .unwrap()
        .push(json!({"tag":"ordinary", "protocol":{"type":"direct"}}));
    input["route"]["auto_outbounds"] = json!(["ordinary"]);
    assert!(RuntimeConfig::parse(&input.to_string())
        .unwrap_err()
        .to_string()
        .contains("does not advertise"));
}

#[test]
fn parses_and_validates_wireguard_outbound() {
    let parsed = RuntimeConfig::parse(&config(valid_protocol())).unwrap();
    let OutboundProtocolConfig::Wireguard {
        addresses,
        mtu,
        peers,
        ..
    } = &parsed.outbounds[0].protocol
    else {
        panic!("expected wireguard outbound")
    };
    assert_eq!(addresses, &["172.16.0.2/32", "fd00::2/128"]);
    assert_eq!(*mtu, 1420);
    assert_eq!(peers[0].keepalive_secs, 25);
    assert_eq!(parsed.outbounds[0].protocol.protocol_name(), "wireguard");
    assert!(parsed.outbounds[0].protocol.endpoint().is_none());
}

#[test]
fn outer_udp_proxy_must_reference_another_outbound() {
    let mut protocol = valid_protocol();
    protocol["outer_udp_proxy"] = json!("outer");
    let config = json!({
        "outbounds": [
            {"tag":"outer", "protocol":{"type":"socks5", "server":"127.0.0.1", "port":1080}},
            {"tag":"wg", "protocol":protocol}
        ],
        "route":{"rules":[],"final":{"type":"direct"}}
    });
    RuntimeConfig::parse(&config.to_string()).expect("valid outer UDP proxy reference");

    let mut self_ref = config.clone();
    self_ref["outbounds"][1]["protocol"]["outer_udp_proxy"] = json!("wg");
    assert!(RuntimeConfig::parse(&self_ref.to_string())
        .unwrap_err()
        .to_string()
        .contains("cannot use itself"));

    let mut missing = config;
    missing["outbounds"][1]["protocol"]["outer_udp_proxy"] = json!("absent");
    assert!(RuntimeConfig::parse(&missing.to_string())
        .unwrap_err()
        .to_string()
        .contains("unknown outer_udp_proxy"));
}

#[test]
fn outer_udp_proxy_accepts_concrete_relay_and_rejects_recursive_or_dynamic_groups() {
    let mut protocol = valid_protocol();
    protocol["outer_udp_proxy"] = json!("outer-chain");
    let mut config = json!({
        "outbounds": [
            {"tag":"first", "protocol":{"type":"socks5", "server":"127.0.0.1", "port":1080}},
            {"tag":"second", "protocol":{"type":"socks5", "server":"127.0.0.1", "port":1081}},
            {"tag":"wg", "protocol":protocol}
        ],
        "outbound_groups": [
            {"tag":"outer-chain", "type":"relay", "proxies":["first", "second"]}
        ],
        "route":{"rules":[],"final":{"type":"direct"}}
    });
    RuntimeConfig::parse(&config.to_string()).expect("concrete outer UDP relay group");

    config["outbound_groups"][0]["proxies"][1] = json!("wg");
    assert!(RuntimeConfig::parse(&config.to_string())
        .unwrap_err()
        .to_string()
        .contains("cannot occur in its outer_udp_proxy relay group"));

    config["outbound_groups"][0]["proxies"][1] = json!("second");
    config["outbound_groups"][0]["type"] = json!("selector");
    config["outbound_groups"][0]["outbounds"] = json!(["first", "second"]);
    config["outbound_groups"][0]
        .as_object_mut()
        .unwrap()
        .remove("proxies");
    assert!(RuntimeConfig::parse(&config.to_string())
        .unwrap_err()
        .to_string()
        .contains("must be a relay group"));
}

#[test]
fn rejects_conflicting_allowed_ip_prefixes() {
    let mut protocol = valid_protocol();
    protocol["peers"] = json!([
        {
            "public_key": key(2),
            "endpoint": "one.example:51820",
            "allowed_ips": ["10.0.0.1/8"]
        },
        {
            "public_key": key(3),
            "endpoint": "two.example:51820",
            "allowed_ips": ["10.1.2.3/8"]
        }
    ]);
    let error = RuntimeConfig::parse(&config(protocol)).unwrap_err();
    assert!(error
        .to_string()
        .contains("peers 0 and 1 declare the same allowed-IP prefix"));
}

#[test]
fn invalid_private_key_is_not_echoed() {
    let secret = "private-value-that-must-not-appear";
    let mut protocol = valid_protocol();
    protocol["private_key"] = json!(secret);
    let error = RuntimeConfig::parse(&config(protocol)).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("private key is invalid"));
    assert!(!message.contains(secret));
}

#[test]
fn rejects_invalid_reserved_length() {
    let mut protocol = valid_protocol();
    protocol["peers"][0]["reserved"] = json!([1, 2]);
    let error = RuntimeConfig::parse(&config(protocol)).unwrap_err();
    assert!(error
        .to_string()
        .contains("reserved must contain exactly 3 bytes"));
}

#[test]
fn configuration_debug_redacts_private_and_pre_shared_keys() {
    let private = key(1);
    let pre_shared = key(9);
    let mut protocol = valid_protocol();
    protocol["peers"][0]["pre_shared_key"] = json!(pre_shared);
    let parsed = RuntimeConfig::parse(&config(protocol)).unwrap();

    let debug = format!("{parsed:?}");
    assert!(!debug.contains(&private));
    assert!(!debug.contains(&pre_shared));
    assert!(debug.contains("WireguardSecret([REDACTED])"));

    let serialized = serde_json::to_value(&parsed.outbounds[0].protocol).unwrap();
    assert_eq!(serialized["private_key"], private);
    assert_eq!(serialized["peers"][0]["pre_shared_key"], pre_shared);
}

#[test]
fn parses_and_validates_wireguard_inbound_without_exposing_keys() {
    let private = key(11);
    let preshared = key(12);
    let config = json!({
        "inbounds": [{
            "tag": "wg-in",
            "listen": {"address": "127.0.0.1", "port": 51820},
            "protocol": {
                "type": "wireguard",
                "private_key": private,
                "peers": [{
                    "public_key": key(13),
                    "pre_shared_key": preshared,
                    "allowed_ips": ["10.0.0.2/32"]
                }]
            }
        }],
        "route": {"rules": [], "final": {"type": "direct"}}
    })
    .to_string();
    let parsed = RuntimeConfig::parse(&config).unwrap();
    let InboundProtocolConfig::Wireguard { mtu, peers, .. } = &parsed.inbounds[0].protocol else {
        panic!("expected WireGuard inbound");
    };
    assert_eq!(*mtu, 1420);
    assert_eq!(peers[0].allowed_ips, ["10.0.0.2/32"]);
    let debug = format!("{parsed:?}");
    assert!(!debug.contains(&private));
    assert!(!debug.contains(&preshared));
}

#[test]
fn rejects_invalid_wireguard_inbound_peer_without_echoing_secret() {
    let secret = "must-not-appear-in-error";
    let config = json!({
        "inbounds": [{
            "tag": "wg-in",
            "listen": {"address": "127.0.0.1", "port": 51820},
            "protocol": {
                "type": "wireguard",
                "private_key": key(11),
                "peers": [{"public_key": secret, "allowed_ips": ["10.0.0.2/32"]}]
            }
        }],
        "route": {"rules": [], "final": {"type": "direct"}}
    })
    .to_string();
    let message = RuntimeConfig::parse(&config).unwrap_err().to_string();
    assert!(message.contains("peer 0 key is invalid"));
    assert!(!message.contains(secret));
}
