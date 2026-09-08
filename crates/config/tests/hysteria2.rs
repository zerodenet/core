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
