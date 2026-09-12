use serde_json::json;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};
fn config(inbound: serde_json::Value, outbound: serde_json::Value) -> String {
    json!({"inbounds":[{"tag":"in","listen":{"address":"127.0.0.1","port":10800},"protocol":inbound}],"outbounds":[{"tag":"out","protocol":outbound}],"route":{"rules":[],"final":{"type":"route","outbound":"out"}}}).to_string()
}
fn ss(inbound: bool) -> serde_json::Value {
    let mut value = json!({"type":"shadowsocks","cipher":"aes-128-gcm","password":"secret"});
    if !inbound {
        value["server"] = json!("127.0.0.1");
        value["port"] = json!(8388);
    }
    value
}
#[test]
fn shadowsocks_native_options_roundtrip_and_defaults_are_explicit() {
    let mut inbound = ss(true);
    let mut outbound = ss(false);
    inbound["replay_attack"] = json!("reject");
    outbound["replay_attack"] = json!("detect");
    inbound["state_limits"] =
        json!({"udp_capacity":4,"udp_timeout_secs":600,"tcp_replay_capacity":512});
    outbound["plugin"] = json!({"command":"example-plugin","args":["--one","two words"],"options":"one=two;three=four","mode":"tcp_and_udp"});
    let parsed = RuntimeConfig::parse(&config(inbound, outbound)).unwrap();
    let roundtrip = RuntimeConfig::parse(&serde_json::to_string(&parsed).unwrap()).unwrap();
    assert_eq!(parsed, roundtrip);
    let defaults = RuntimeConfig::parse(&config(ss(true), ss(false))).unwrap();
    let InboundProtocolConfig::Shadowsocks { state_limits, .. } = &defaults.inbounds[0].protocol
    else {
        panic!()
    };
    assert_eq!(state_limits.udp_timeout_secs, 300);
    assert_eq!(state_limits.udp_capacity, None);
    let OutboundProtocolConfig::Shadowsocks {
        state_limits,
        plugin,
        ..
    } = &defaults.outbounds[0].protocol
    else {
        panic!()
    };
    assert_eq!(state_limits.tcp_replay_capacity, None);
    assert!(plugin.is_none());
}
#[test]
fn shadowsocks_rejects_invalid_policy_limits_and_plugin_fields_before_start() {
    for (field, value) in [
        ("replay_attack", json!("accept")),
        ("state_limits", json!({"udp_timeout_secs":0})),
        ("state_limits", json!({"udp_capacity":0})),
        ("state_limits", json!({"tcp_replay_capacity":0})),
        ("plugin", json!({"command":""})),
        ("plugin", json!({"command":"plugin","mode":"invalid"})),
        ("plugin", json!({"command":"plugin","unknown":true})),
    ] {
        let mut inbound = ss(true);
        inbound[field] = value.clone();
        assert!(
            RuntimeConfig::parse(&config(inbound, ss(false))).is_err(),
            "{field}={value}"
        );
        let mut outbound = ss(false);
        outbound[field] = value.clone();
        assert!(
            RuntimeConfig::parse(&config(ss(true), outbound)).is_err(),
            "{field}={value}"
        );
    }
}
#[test]
fn shadowsocks_accepts_plain_without_password_and_case_insensitive_methods() {
    for method in ["none", "plain"] {
        let inbound = json!({"type":"shadowsocks","cipher":method});
        let outbound =
            json!({"type":"shadowsocks","cipher":method,"server":"127.0.0.1","port":8388});
        assert!(RuntimeConfig::parse(&config(inbound, outbound)).is_ok());
    }
    let mut inbound = ss(true);
    inbound["cipher"] = json!("AES-128-GCM");
    assert!(RuntimeConfig::parse(&config(inbound, ss(false))).is_ok());
    let inbound = json!({"type":"shadowsocks","cipher":"aes-128-cfb","users":[{"password":"a"},{"password":"b"}]});
    assert!(RuntimeConfig::parse(&config(inbound, ss(false))).is_err());
}

#[test]
fn explicit_empty_v1_user_preserves_reference_credentials_without_enabling_empty_registries() {
    let config = serde_json::json!({"inbounds":[{"tag":"ss","listen":{"address":"127.0.0.1","port":8388},"protocol":{"type":"shadowsocks","cipher":"aes-128-gcm","users":[{"password":""}]}}],"route":{"rules":[],"final":{"type":"direct"}}});
    RuntimeConfig::parse(&config.to_string()).unwrap();
}
