use serde_json::json;
use zero_config::RuntimeConfig;
fn parse(queue: serde_json::Value) -> Result<RuntimeConfig, zero_config::ConfigError> {
    RuntimeConfig::parse(
        &json!({"runtime":{"network":{"udp_receive_queue":queue}},
        "route":{"rules":[],"final":{"type":"direct"}}})
        .to_string(),
    )
}
#[test]
fn udp_receive_queue_defaults_preserve_slot_only_admission() {
    let config = parse(json!({})).unwrap();
    let queue = &config.runtime.network.udp_receive_queue;
    assert_eq!(queue.capacity, 64);
    assert_eq!(queue.max_payload_bytes, None);
    assert_eq!(queue.max_stack_payload_bytes, None);
    let absent =
        RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap();
    assert_eq!(queue, &absent.runtime.network.udp_receive_queue);
}
#[test]
fn udp_receive_queue_overrides_round_trip_through_canonical_config() {
    let config = parse(json!({"capacity":128,"max_payload_bytes":8388608,
        "max_stack_payload_bytes":67108864}))
    .unwrap();
    let encoded = serde_json::to_string(&config).unwrap();
    let roundtrip = RuntimeConfig::parse(&encoded).unwrap();
    assert_eq!(config, roundtrip);
    assert_eq!(roundtrip.runtime.network.udp_receive_queue.capacity, 128);
    assert_eq!(
        parse(json!({"capacity":65536,"max_payload_bytes":65527}))
            .unwrap()
            .runtime
            .network
            .udp_receive_queue
            .max_payload_bytes,
        Some(65527)
    );
}
#[test]
fn udp_receive_queue_rejects_invalid_limits_and_unknown_keys() {
    for invalid in [
        json!({"capacity":0}),
        json!({"capacity":65537}),
        json!({"capacity":-1}),
        json!({"max_payload_bytes":0}),
        json!({"max_stack_payload_bytes":65526}),
        json!({"max_payload_bytes":65526}),
        json!({"bytes":65527}),
    ] {
        assert!(parse(invalid.clone()).is_err(), "{invalid}");
    }
    assert!(parse(json!({"max_payload_bytes":null,"max_stack_payload_bytes":null})).is_ok());
}
