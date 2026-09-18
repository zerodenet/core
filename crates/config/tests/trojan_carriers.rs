use zero_config::{InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};

fn parse(inbound_carrier: &str, outbound_carrier: &str) -> RuntimeConfig {
    RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "in",
                "listen": {{"address": "127.0.0.1", "port": 443}},
                "protocol": {{
                    "type": "trojan",
                    "password": "secret",
                    "tls": {{"cert_path": "cert.pem", "key_path": "key.pem"}}
                    {inbound_carrier}
                }}
            }}],
            "outbounds": [{{
                "tag": "out",
                "protocol": {{
                    "type": "trojan",
                    "server": "example.com",
                    "port": 443,
                    "password": "secret"
                    {outbound_carrier}
                }}
            }}],
            "route": {{"rules": [], "final": {{"type": "route", "outbound": "out"}}}}
        }}"#
    ))
    .expect("parse Trojan carrier config")
}

#[test]
fn websocket_carrier_is_available_in_both_directions() {
    let config = parse(
        r#", "ws": {"path": "/trojan"}"#,
        r#", "ws": {"path": "/trojan"}"#,
    );
    assert!(matches!(
        &config.inbounds[0].protocol,
        InboundProtocolConfig::Trojan { ws: Some(_), .. }
    ));
    assert!(matches!(
        &config.outbounds[0].protocol,
        OutboundProtocolConfig::Trojan { ws: Some(_), .. }
    ));
}

#[test]
fn grpc_carrier_is_available_in_both_directions() {
    let config = parse(
        r#", "grpc": {"service_names": ["/zero.trojan/Tun"]}"#,
        r#", "grpc": {"service_names": ["/zero.trojan/Tun"]}"#,
    );
    assert!(matches!(
        &config.inbounds[0].protocol,
        InboundProtocolConfig::Trojan { grpc: Some(_), .. }
    ));
    assert!(matches!(
        &config.outbounds[0].protocol,
        OutboundProtocolConfig::Trojan { grpc: Some(_), .. }
    ));
}

#[test]
fn websocket_and_grpc_are_mutually_exclusive() {
    for inbound in [true, false] {
        let both =
            r#", "ws": {"path": "/trojan"}, "grpc": {"service_names": ["/zero.trojan/Tun"]}"#;
        let result = if inbound {
            RuntimeConfig::parse(&format!(
                r#"{{"inbounds":[{{"tag":"in","listen":{{"address":"127.0.0.1","port":443}},"protocol":{{"type":"trojan","password":"secret","tls":{{"cert_path":"cert.pem","key_path":"key.pem"}}{both}}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#
            ))
        } else {
            RuntimeConfig::parse(&format!(
                r#"{{"outbounds":[{{"tag":"out","protocol":{{"type":"trojan","server":"example.com","port":443,"password":"secret"{both}}}}}],"route":{{"rules":[],"final":{{"type":"route","outbound":"out"}}}}}}"#
            ))
        };
        assert!(result.is_err());
    }
}
