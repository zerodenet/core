use serde_json::json;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};

#[test]
fn parses_hysteria2_salamander_hopping_and_node_tls_identity() {
    let raw = json!({
        "inbounds": [{
            "tag": "hy2-in",
            "listen": { "address": "127.0.0.1", "port": 4433 },
            "protocol": {
                "type": "hysteria2",
                "password": "secret",
                "transport": {
                    "obfs": { "type": "salamander", "password": "mask-secret" }
                }
            }
        }],
        "outbounds": [{
            "tag": "hy2-out",
            "protocol": {
                "type": "hysteria2",
                "server": "node.example",
                "port": 443,
                "password": "secret",
                "ca_cert_path": "certs/node-ca.pem",
                "tls_options": {
                    "disable_system_roots": true,
                    "pinned_peer_cert_sha256": [
                        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
                    ]
                },
                "transport": {
                    "obfs": { "type": "salamander", "password": "mask-secret" },
                    "udp_hop": {
                        "ports": [443, 8443],
                        "interval_min_secs": 5,
                        "interval_max_secs": 10
                    }
                }
            }
        }],
        "route": { "rules": [], "final": { "type": "route", "outbound": "hy2-out" } }
    });
    let config = RuntimeConfig::parse(&raw.to_string()).expect("parse Hysteria2 node transport");

    let InboundProtocolConfig::Hysteria2 { transport, .. } = &config.inbounds[0].protocol else {
        panic!("expected Hysteria2 inbound");
    };
    assert_eq!(
        transport
            .obfs
            .as_ref()
            .expect("inbound obfs")
            .salamander_password(),
        "mask-secret"
    );

    let OutboundProtocolConfig::Hysteria2 {
        transport,
        ca_cert_path,
        tls_options,
        ..
    } = &config.outbounds[0].protocol
    else {
        panic!("expected Hysteria2 outbound");
    };
    assert_eq!(ca_cert_path.as_deref(), Some("certs/node-ca.pem"));
    assert!(tls_options.disable_system_roots);
    assert_eq!(
        transport.udp_hop.as_ref().expect("udp hop").ports,
        [443, 8443]
    );
}

#[test]
fn rejects_inbound_port_hopping_and_invalid_salamander_password() {
    for protocol in [
        json!({
            "type": "hysteria2",
            "password": "secret",
            "transport": { "udp_hop": { "ports": [443] } }
        }),
        json!({
            "type": "hysteria2",
            "password": "secret",
            "transport": { "obfs": { "type": "salamander", "password": "abc" } }
        }),
    ] {
        let raw = json!({
            "inbounds": [{
                "tag": "hy2-in",
                "listen": { "address": "127.0.0.1", "port": 4433 },
                "protocol": protocol
            }],
            "outbounds": [],
            "route": { "rules": [], "final": { "type": "direct" } }
        });
        let error = RuntimeConfig::parse(&raw.to_string())
            .expect_err("invalid Hysteria2 transport must fail");
        assert!(error.to_string().contains("hysteria2") || error.to_string().contains("Hysteria2"));
    }
}

#[test]
fn rejects_invalid_hysteria2_node_pin_and_hop_interval() {
    for fields in [
        json!({ "tls_options": { "pinned_peer_cert_sha256": ["AA=="] } }),
        json!({
            "transport": {
                "udp_hop": {
                    "ports": [443],
                    "interval_min_secs": 4,
                    "interval_max_secs": 5
                }
            }
        }),
    ] {
        let mut protocol = json!({
            "type": "hysteria2",
            "server": "node.example",
            "port": 443,
            "password": "secret"
        });
        protocol
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let raw = json!({
            "inbounds": [],
            "outbounds": [{ "tag": "hy2-out", "protocol": protocol }],
            "route": { "rules": [], "final": { "type": "route", "outbound": "hy2-out" } }
        });
        RuntimeConfig::parse(&raw.to_string())
            .expect_err("invalid Hysteria2 node settings must fail");
    }
}
