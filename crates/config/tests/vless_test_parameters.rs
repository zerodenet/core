use serde_json::json;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};

fn config() -> serde_json::Value {
    json!({
        "inbounds": [{
            "tag": "in",
            "listen": {"address": "127.0.0.1", "port": 12001},
            "protocol": {
                "type": "vless",
                "users": [{
                    "id": "00112233-4455-6677-8899-aabbccddeeff",
                    "testseed": [10, 1, 20, 1]
                }]
            }
        }],
        "outbounds": [{
            "tag": "out",
            "protocol": {
                "type": "vless",
                "server": "127.0.0.1",
                "port": 12002,
                "id": "00112233-4455-6677-8899-aabbccddeeff",
                "testpre": 2,
                "testseed": [11, 2, 21, 3]
            }
        }],
        "route": {"rules": [], "final": {"type": "route", "outbound": "out"}}
    })
}

#[test]
fn vless_test_parameters_parse_and_roundtrip() {
    let parsed = RuntimeConfig::parse(&config().to_string()).unwrap();
    let InboundProtocolConfig::Vless { users, .. } = &parsed.inbounds[0].protocol else {
        panic!("expected VLESS inbound");
    };
    assert_eq!(users[0].testseed, [10, 1, 20, 1]);
    let OutboundProtocolConfig::Vless {
        testpre, testseed, ..
    } = &parsed.outbounds[0].protocol
    else {
        panic!("expected VLESS outbound");
    };
    assert_eq!(*testpre, 2);
    assert_eq!(testseed.as_slice(), &[11, 2, 21, 3]);

    let encoded = serde_json::to_string(&parsed).unwrap();
    assert_eq!(RuntimeConfig::parse(&encoded).unwrap(), parsed);
}

#[test]
fn vless_testseed_rejects_invalid_xray_ranges() {
    let mut value = config();
    value["outbounds"][0]["protocol"]["testseed"] = json!([900, 0, 900, 256]);
    assert!(RuntimeConfig::parse(&value.to_string())
        .unwrap_err()
        .to_string()
        .contains("random ranges"));

    let mut value = config();
    value["inbounds"][0]["protocol"]["users"][0]["testseed"] = json!([901, 500, 900, 256]);
    assert!(RuntimeConfig::parse(&value.to_string())
        .unwrap_err()
        .to_string()
        .contains("threshold"));
}

#[test]
fn short_vless_testseed_uses_xray_default_compatibility() {
    let mut value = config();
    value["outbounds"][0]["protocol"]["testseed"] = json!([0, 0, 0]);
    RuntimeConfig::parse(&value.to_string()).unwrap();
    assert_eq!(
        vless::validation::normalize_vision_testseed(&[0, 0, 0]).unwrap(),
        [900, 500, 900, 256]
    );
}
