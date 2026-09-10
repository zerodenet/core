use zero_config::{InboundProtocolConfig, MieruTransport, OutboundProtocolConfig, RuntimeConfig};
fn config(field: &str) -> String {
    format!(
        r#"{{"inbounds":[{{"tag":"in","listen":{{"address":"127.0.0.1","port":8964}},"protocol":{{"type":"mieru","users":[{{"username":"u","password":"p"}}]{field}}}}}],"outbounds":[{{"tag":"out","protocol":{{"type":"mieru","server":"127.0.0.1","port":8965,"username":"u","password":"p"{field}}}}}],"route":{{"rules":[],"final":{{"type":"route","outbound":"out"}}}}}}"#
    )
}
#[test]
fn mieru_defaults_to_tcp_and_round_trips_native_udp_carrier() {
    for (field, expected) in [
        ("", MieruTransport::Tcp),
        (r#", "transport":"udp""#, MieruTransport::Udp),
    ] {
        let config = RuntimeConfig::parse(&config(field)).unwrap();
        assert!(
            matches!(config.inbounds[0].protocol,InboundProtocolConfig::Mieru {transport,..} if transport==expected)
        );
        assert!(
            matches!(config.outbounds[0].protocol,OutboundProtocolConfig::Mieru {transport,..} if transport==expected)
        );
        let serialized = serde_json::to_string(&config).unwrap();
        assert_eq!(RuntimeConfig::parse(&serialized).unwrap(), config);
    }
}
#[test]
fn mieru_rejects_unknown_carrier_and_duplicate_listener() {
    assert!(RuntimeConfig::parse(&config(r#", "transport":"quic""#)).is_err());
    let mut value: serde_json::Value =
        serde_json::from_str(&config(r#", "transport":"udp""#)).unwrap();
    let mut inbound = value["inbounds"][0].clone();
    inbound["tag"] = "duplicate".into();
    value["inbounds"].as_array_mut().unwrap().push(inbound);
    assert!(RuntimeConfig::parse(&value.to_string()).is_err());
}

#[test]
fn mieru_mtu_traffic_and_receive_options_round_trip_on_both_directions() {
    let field = r#", "mtu":1280,"traffic_pattern":{"seed":123,"tcp_fragment":{"enable":true,"max_sleep_ms":2},"nonce":{"type":"fixed","apply_to_all_udp_packet":true,"custom_hex_strings":["01020304"]}},"receive":{"stall_timeout_ms":45000}"#;
    let parsed = RuntimeConfig::parse(&config(field)).unwrap();
    let InboundProtocolConfig::Mieru { options, .. } = &parsed.inbounds[0].protocol else {
        panic!()
    };
    assert_eq!(options.mtu, 1280);
    assert_eq!(options.receive.stall_timeout_ms, 45000);
    let OutboundProtocolConfig::Mieru {
        options: outbound, ..
    } = &parsed.outbounds[0].protocol
    else {
        panic!()
    };
    assert_eq!(options, outbound);
    let serialized = serde_json::to_string(&parsed).unwrap();
    assert_eq!(RuntimeConfig::parse(&serialized).unwrap(), parsed);
}

#[test]
fn mieru_rejects_invalid_options_before_runtime_start() {
    for field in [
        r#", "mtu":1200"#,
        r#", "traffic_pattern":{"nonce":{"min_len":13}}"#,
        r#", "receive":{"stall_timeout_ms":999}"#,
    ] {
        assert!(RuntimeConfig::parse(&config(field)).is_err(), "{field}");
    }
}
