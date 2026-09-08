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

fn outbound_with_transport(
    transport: serde_json::Value,
) -> Result<RuntimeConfig, zero_config::ConfigError> {
    RuntimeConfig::parse(&serde_json::json!({"inbounds": [], "outbounds": [{"tag":"hy", "protocol":{"type":"hysteria2", "server":"192.0.2.1", "port":443, "password":"test", "transport":transport}}], "route":{"rules":[],"final":{"type":"route","outbound":"hy"}}}).to_string())
}
#[test]
fn hysteria2_transport_configuration_is_validated_and_round_trips() {
    let transport = serde_json::json!({"bandwidth":{"up":"100 Mbps","down":"1 Gbps","disable_loss_compensation":true},"congestion":{"type":"reno"},"quic":{"stream_receive_window":1048576,"connection_receive_window":4194304,"keep_alive_interval_secs":5,"disable_path_mtu_discovery":true}});
    let config = outbound_with_transport(transport).unwrap();
    let encoded = serde_json::to_string(&config).unwrap();
    assert_eq!(RuntimeConfig::parse(&encoded).unwrap(), config);
    let OutboundProtocolConfig::Hysteria2 { transport, .. } = &config.outbounds[0].protocol else {
        panic!()
    };
    let settings = transport.validated().unwrap();
    assert_eq!(settings.upload, 12_500_000);
    assert_eq!(settings.download, 125_000_000);
    assert!(settings.quic.disable_path_mtu_discovery);
}
#[test]
fn hysteria2_rejects_invalid_transport_values() {
    for transport in [
        serde_json::json!({"bandwidth":{"up":"1 Mbpss"}}),
        serde_json::json!({"quic":{"stream_receive_window":1}}),
        serde_json::json!({"quic":{"max_idle_timeout_secs":4,"keep_alive_interval_secs":10}}),
        serde_json::json!({"congestion":{"type":"brutal"}}),
        serde_json::json!({"congestion":{"bbr_initial_window":0}}),
        serde_json::json!({"quic":{"unknown":true}}),
    ] {
        assert!(
            outbound_with_transport(transport.clone()).is_err(),
            "{transport}"
        );
    }
}

#[test]
fn hysteria2_bandwidth_accepts_integer_bytes_per_second() {
    let config =
        outbound_with_transport(serde_json::json!({"bandwidth":{"up":1000000,"down":"8 mbps"}}))
            .unwrap();
    let OutboundProtocolConfig::Hysteria2 { transport, .. } = &config.outbounds[0].protocol else {
        panic!()
    };
    let settings = transport.validated().unwrap();
    assert_eq!(settings.upload, settings.download);
    assert_eq!(settings.upload, 1_000_000);
}
