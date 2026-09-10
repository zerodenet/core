use mieru_config::MieruTransportOptions;

#[test]
fn mtu_budget_includes_outer_ip_udp_and_both_aead_tags() {
    for mtu in [1280, 1400, 1500] {
        let options = MieruTransportOptions {
            mtu,
            ..Default::default()
        };
        options.validate().unwrap();
        for (ipv6, ip_header) in [(false, 20), (true, 40)] {
            assert_eq!(
                options.fragment_size(ipv6) + 88 + 8 + ip_header,
                mtu as usize
            );
        }
    }
    for mtu in [0, 1279, 1501, u16::MAX] {
        assert!(MieruTransportOptions {
            mtu,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
}

#[test]
fn traffic_patterns_and_receive_policy_reject_unsafe_bounds() {
    for json in [
        r#"{"traffic_pattern":{"tcp_fragment":{"max_sleep_ms":101}}}"#,
        r#"{"traffic_pattern":{"nonce":{"min_len":13}}}"#,
        r#"{"traffic_pattern":{"nonce":{"min_len":9,"max_len":8}}}"#,
        r#"{"traffic_pattern":{"nonce":{"type":"fixed","custom_hex_strings":["xx"]}}}"#,
        r#"{"traffic_pattern":{"nonce":{"custom_hex_strings":["f"]}}}"#,
        r#"{"receive":{"stall_timeout_ms":0}}"#,
        r#"{"receive":{"max_pending_bytes":0}}"#,
        r#"{"receive":{"max_pending_frames":0}}"#,
        r#"{"receive":{"max_connection_pending_bytes":1}}"#,
    ] {
        let options: MieruTransportOptions = serde_json::from_str(json).unwrap();
        assert!(options.validate().is_err(), "{json}");
    }
    // v3.33.0 permits a fixed pattern with no prefixes, meaning random.
    let options: MieruTransportOptions =
        serde_json::from_str(r#"{"traffic_pattern":{"nonce":{"type":"fixed"}}}"#).unwrap();
    options.validate().unwrap();
}
