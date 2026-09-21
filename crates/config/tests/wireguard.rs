use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use zero_config::{OutboundProtocolConfig, RuntimeConfig};

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
