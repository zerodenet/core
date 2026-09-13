use serde_json::{json, Value};
use zero_config::RuntimeConfig;
const PRIVATE: &str = "OKMOFBeltHBXaTQ8cIcsgabVQcqXeTB9Ih3lPtWMY04";
const PUBLIC: &str = "9AwHi13y1rN6EWTSo8-HNCOhrzr251jNY7SSIxo0diA";
const SEED: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
fn inbound(reality: Value) -> Value {
    json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":8443},"protocol":{"type":"vless","users":[{"id":"test-user"}],"reality":reality}}],"route":{"final":{"type":"direct"}}})
}
fn base() -> Value {
    json!({"private_key":PRIVATE,"short_ids":[""],"server_names":["one.test","two.test"],"min_client_version":"26.3","max_client_version":"26.3.27","max_time_diff_ms":1500,"mldsa65_seed":SEED,"target":{"destination":{"type":"tcp","server":"origin.test","port":443},"proxy_protocol":2,"upload":{"after_bytes":1024,"bytes_per_sec":2048,"burst_bytes":4096},"download":{"bytes_per_sec":4096}}})
}
#[test]
fn parses_native_reality_target_policy_and_crypto_extensions() {
    let raw = inbound(base());
    let config = RuntimeConfig::parse(&raw.to_string()).unwrap();
    let serialized = serde_json::to_value(config).unwrap();
    let reality = &serialized["inbounds"][0]["protocol"]["reality"];
    assert_eq!(reality["server_names"], json!(["one.test", "two.test"]));
    assert_eq!(reality["target"]["upload"]["bytes_per_sec"], 2048);
    assert_eq!(reality["max_time_diff_ms"], 1500);
    let mut unix = base();
    unix["target"]["destination"] = json!({"type":"unix","path":"targets/reality.sock"});
    RuntimeConfig::parse(&inbound(unix).to_string()).unwrap();
}
#[test]
fn rejects_invalid_policy_seed_and_target_before_listening() {
    for (field, value) in [
        ("min_client_version", json!("26.300")),
        ("max_client_version", json!("25")),
        ("mldsa65_seed", json!(PRIVATE)),
        ("mldsa65_seed", json!("AA")),
        ("short_ids", json!([])),
        ("server_names", json!(["*.example"])),
    ] {
        let mut reality = base();
        reality[field] = value;
        assert!(
            RuntimeConfig::parse(&inbound(reality).to_string()).is_err(),
            "{field}"
        );
    }
    for target in [
        json!({"destination":{"type":"tcp","server":"origin.test","port":0}}),
        json!({"destination":{"type":"unix","path":""}}),
        json!({"destination":{"type":"tcp","server":"origin.test","port":443},"proxy_protocol":3}),
    ] {
        let mut reality = base();
        reality["target"] = target;
        assert!(RuntimeConfig::parse(&inbound(reality).to_string()).is_err());
    }
}
#[test]
fn client_password_alias_and_verification_key_are_validated_without_dataplane() {
    let verify =
        include_str!("../../../protocols/vless/tests/fixtures/reality_mldsa65_zero_seed.txt")
            .trim();
    let mut raw = json!({"outbounds":[{"tag":"vless","protocol":{"type":"vless","server":"proxy.test","port":443,"id":"test-user","reality":{"password":PUBLIC,"mldsa65_verify":verify,"short_id":"","server_name":"one.test"}}}],"route":{"final":{"type":"route","outbound":"vless"}}});
    RuntimeConfig::parse(&raw.to_string()).unwrap();
    raw["outbounds"][0]["protocol"]["reality"]["mldsa65_verify"] = json!(PUBLIC);
    assert!(RuntimeConfig::parse(&raw.to_string()).is_err());
}

#[test]
fn reality_accepts_versioned_and_random_fingerprints_before_runtime_construction() {
    for name in [
        "ios",
        "ios-13",
        "qq-11.1",
        "chrome-133",
        "firefox-148",
        "safari-26.3",
        "edge-85",
        "random",
        "randomized",
        "randomizednoalpn",
    ] {
        let raw = json!({"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"proxy.test","port":443,"id":"test-user","reality":{"public_key":PUBLIC,"short_id":"","server_name":"example.com","client_fingerprint":name}}}],"route":{"final":{"type":"route","outbound":"node"}}});
        RuntimeConfig::parse(&raw.to_string()).unwrap_or_else(|error| panic!("{name}: {error}"));
    }
}
