use serde_json::json;
use zero_config::RuntimeConfig;
const KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE";
#[test]
fn encryption_configuration_is_protocol_validated_and_roundtrips() {
    let decryption = format!("mlkem768x25519plus.random.600s.{KEY}");
    let encryption = format!("mlkem768x25519plus.random.0rtt.{KEY}");
    let mut config = json!({"inbounds":[{"tag":"in","listen":{"address":"127.0.0.1","port":10001},"protocol":{"type":"vless","users":[{"id":"test","flow":"xtls-rprx-vision"}],"decryption":decryption}}],"outbounds":[{"tag":"out","protocol":{"type":"vless","server":"127.0.0.1","port":10002,"id":"test","encryption":encryption,"flow":"xtls-rprx-vision-udp443"}}],"route":{"rules":[],"final":{"type":"route","outbound":"out"}}});
    let parsed = RuntimeConfig::parse(&config.to_string()).unwrap();
    let encoded = serde_json::to_string(&parsed).unwrap();
    assert_eq!(RuntimeConfig::parse(&encoded).unwrap(), parsed);
    config["inbounds"][0]["protocol"]["decryption"] =
        json!("mlkem768x25519plus.random.600s.short-key");
    assert!(RuntimeConfig::parse(&config.to_string()).is_err());
}
