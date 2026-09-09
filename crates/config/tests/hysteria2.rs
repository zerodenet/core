use zero_config::{OutboundProtocolConfig, RuntimeConfig};

#[test]
fn hysteria2_server_name_is_independent_of_dial_address() {
    let config = RuntimeConfig::parse(r#"{
        "inbounds": [],
        "outbounds": [{"tag": "hy", "protocol": {"type": "hysteria2", "server": "192.0.2.1", "port": 443, "password": "test", "server_name": "proxy.example"}}],
        "route": {"rules": [], "final": {"type": "route", "outbound": "hy"}}
    }"#).unwrap();
    let OutboundProtocolConfig::Hysteria2 {
        server,
        server_name,
        insecure,
        ..
    } = &config.outbounds[0].protocol
    else {
        panic!("expected hysteria2")
    };
    assert_eq!(server, "192.0.2.1");
    assert_eq!(server_name.as_deref(), Some("proxy.example"));
    assert!(!insecure);
}

#[test]
fn hysteria2_rejects_empty_server_name() {
    let result = RuntimeConfig::parse(
        r#"{
        "inbounds": [],
        "outbounds": [{"tag": "hy", "protocol": {"type": "hysteria2", "server": "192.0.2.1", "port": 443, "password": "test", "server_name": ""}}],
        "route": {"rules": [], "final": {"type": "route", "outbound": "hy"}}
    }"#,
    );
    assert!(result.is_err());
}

fn outbound_with_fields(
    fields: serde_json::Value,
) -> Result<RuntimeConfig, zero_config::ConfigError> {
    let mut protocol = serde_json::json!({"type":"hysteria2", "server":"192.0.2.1", "port":443, "password":"test"});
    protocol
        .as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    RuntimeConfig::parse(&serde_json::json!({"inbounds": [], "outbounds": [{"tag":"hy", "protocol":protocol}], "route":{"rules":[],"final":{"type":"route","outbound":"hy"}}}).to_string())
}

#[test]
fn unified_connection_rates_map_to_hysteria_and_round_trip_without_bandwidth_fields() {
    let config = outbound_with_fields(serde_json::json!({
        "up_bps": 12_500_000, "down_bps": 125_000_000,
        "transport":{"congestion":{"type":"reno", "disable_loss_compensation":true},"quic":{"disable_path_mtu_discovery":true}}
    })).unwrap();
    let encoded = serde_json::to_string(&config).unwrap();
    assert!(!encoded.contains("\"bandwidth\""));
    assert_eq!(RuntimeConfig::parse(&encoded).unwrap(), config);
    let OutboundProtocolConfig::Hysteria2 {
        transport,
        up_bps,
        down_bps,
        ..
    } = &config.outbounds[0].protocol
    else {
        panic!()
    };
    let settings = transport.validated(*up_bps, *down_bps).unwrap();
    assert_eq!(settings.upload, 12_500_000);
    assert_eq!(settings.download, 125_000_000);
    assert!(settings.disable_loss_compensation);
    assert!(settings.quic.disable_path_mtu_discovery);
}

#[test]
fn inbound_connection_rates_keep_zero_upload_direction() {
    let config = RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"hy", "listen":{"address":"127.0.0.1","port":443}, "protocol":{"type":"hysteria2","password":"test","up_bps":1_000_000,"down_bps":2_000_000}}],
        "outbounds":[], "route":{"rules":[],"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let protocol = &config.inbounds[0].protocol;
    assert_eq!(protocol.rate_limits(), (None, None));
    let zero_config::InboundProtocolConfig::Hysteria2 {
        transport,
        up_bps,
        down_bps,
        ..
    } = protocol
    else {
        panic!()
    };
    let settings = transport.validated(*down_bps, *up_bps).unwrap();
    assert_eq!(
        settings.upload, 2_000_000,
        "server sends the Zero download direction"
    );
    assert_eq!(
        settings.download, 1_000_000,
        "server receives the Zero upload direction"
    );
}

#[test]
fn zero_rates_have_one_numeric_byte_unit_and_no_official_application_minimum() {
    for rate in [
        serde_json::Value::Null,
        serde_json::json!(0),
        serde_json::json!(1),
        serde_json::json!(65_535),
    ] {
        assert!(
            outbound_with_fields(serde_json::json!({"up_bps": rate, "down_bps": rate})).is_ok()
        );
    }
    for rate in [
        serde_json::json!(-1),
        serde_json::json!("100 Mbps"),
        serde_json::json!(1.5),
        serde_json::json!(u64::MAX),
    ] {
        assert!(outbound_with_fields(serde_json::json!({"up_bps": rate})).is_err());
        assert!(outbound_with_fields(serde_json::json!({"down_bps": rate})).is_err());
    }
}

#[test]
fn hysteria2_rejects_duplicate_bandwidth_surface_and_invalid_transport_values() {
    for transport in [
        serde_json::json!({"bandwidth":{"up":1_000_000}}),
        serde_json::json!({"quic":{"stream_receive_window":1}}),
        serde_json::json!({"quic":{"max_idle_timeout_secs":4,"keep_alive_interval_secs":10}}),
        serde_json::json!({"congestion":{"type":"brutal"}}),
        serde_json::json!({"congestion":{"bbr_initial_window":0}}),
        serde_json::json!({"quic":{"unknown":true}}),
    ] {
        assert!(
            outbound_with_fields(serde_json::json!({"transport":transport})).is_err(),
            "{transport}"
        );
    }
}

#[test]
fn bbr_profiles_validate_and_preserve_independent_initial_window() {
    for (name, profile) in [
        ("standard", hysteria2::settings::BbrProfile::Standard),
        (
            "conservative",
            hysteria2::settings::BbrProfile::Conservative,
        ),
        ("aggressive", hysteria2::settings::BbrProfile::Aggressive),
    ] {
        let config = outbound_with_fields(serde_json::json!({
            "transport":{"congestion":{"bbr_profile":name,"bbr_initial_window":48_000}}
        }))
        .unwrap();
        let OutboundProtocolConfig::Hysteria2 { transport, .. } = &config.outbounds[0].protocol
        else {
            panic!()
        };
        let settings = transport.validated(None, None).unwrap();
        assert_eq!(settings.bbr_profile, profile);
        assert_eq!(settings.bbr_initial_window, 48_000);
        let encoded = serde_json::to_string(&config).unwrap();
        assert_eq!(RuntimeConfig::parse(&encoded).unwrap(), config);
    }
    assert!(outbound_with_fields(
        serde_json::json!({"transport":{"congestion":{"bbr_profile":"turbo"}}})
    )
    .is_err());
}

#[test]
fn receive_window_bounds_preserve_fixed_configs_and_validate_adaptive_ranges() {
    for quic in [
        serde_json::json!({}),
        serde_json::json!({"stream_receive_window":32768,"connection_receive_window":65536}),
    ] {
        let config = outbound_with_fields(serde_json::json!({"transport":{"quic":quic}})).unwrap();
        let OutboundProtocolConfig::Hysteria2 { transport, .. } = &config.outbounds[0].protocol
        else {
            panic!()
        };
        let settings = transport.validated(None, None).unwrap();
        assert_eq!(settings.quic.max_stream_receive_window, None);
        assert_eq!(settings.quic.max_connection_receive_window, None);
    }
    let quic = serde_json::json!({"stream_receive_window":32768,"connection_receive_window":65536,
        "max_stream_receive_window":131072,"max_connection_receive_window":262144});
    let config = outbound_with_fields(serde_json::json!({"transport":{"quic":quic}})).unwrap();
    assert_eq!(
        RuntimeConfig::parse(&serde_json::to_string(&config).unwrap()).unwrap(),
        config
    );
    let OutboundProtocolConfig::Hysteria2 { transport, .. } = &config.outbounds[0].protocol else {
        panic!()
    };
    let settings = transport.validated(None, None).unwrap();
    assert_eq!(settings.quic.stream_receive_window, 32768);
    assert_eq!(settings.quic.max_stream_receive_window, Some(131072));
    assert_eq!(settings.quic.max_connection_receive_window, Some(262144));
    for quic in [
        serde_json::json!({"max_stream_receive_window":0}),
        serde_json::json!({"max_connection_receive_window":16384}),
        serde_json::json!({"max_stream_receive_window":(1u64<<60)+1}),
    ] {
        assert!(outbound_with_fields(serde_json::json!({"transport":{"quic":quic}})).is_err());
    }
}
