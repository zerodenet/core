use serde_json::json;
use zero_config::{ConfigError, RuntimeConfig};

fn config(left: &str, right: &str, right_port: u16) -> Result<RuntimeConfig, ConfigError> {
    RuntimeConfig::parse(&json!({
        "inbounds": [
            {"tag":"left", "listen":{"address":left,"port":8080}, "protocol":{"type":"direct"}},
            {"tag":"right", "listen":{"address":right,"port":right_port}, "protocol":{"type":"direct"}}
        ],
        "route":{"final":{"type":"direct"}}
    }).to_string())
}

#[test]
fn equivalent_ipv6_and_mapped_ipv4_listeners_conflict() {
    for (left, right) in [
        ("::1", "[::1]"),
        ("::1", "0:0:0:0:0:0:0:1"),
        ("[2001:DB8::1]", "2001:db8:0:0:0:0:0:1"),
        ("127.0.0.1", "::ffff:127.0.0.1"),
        ("[::ffff:7f00:1]", "127.0.0.1"),
        ("127.0.0.1", "127.0.0.1"),
        ("LOCALHOST", "localhost"),
    ] {
        for (left, right) in [(left, right), (right, left)] {
            assert!(
                matches!(
                    config(left, right, 8080),
                    Err(ConfigError::DuplicateInboundListen { .. })
                ),
                "{left} / {right}"
            );
            assert!(
                config(left, right, 8081).is_ok(),
                "different ports: {left} / {right}"
            );
        }
    }
}

#[test]
fn wildcard_listener_pairs_are_rejected_portably() {
    for (left, right) in [
        ("::", "0.0.0.0"),
        ("[::]", "127.0.0.1"),
        ("::", "::1"),
        ("0.0.0.0", "127.0.0.1"),
    ] {
        assert!(
            matches!(
                config(left, right, 8080),
                Err(ConfigError::DuplicateInboundListen { .. })
            ),
            "{left} / {right}"
        );
    }
    for (left, right) in [
        ("::1", "::2"),
        ("127.0.0.1", "127.0.0.2"),
        ("127.0.0.1", "::1"),
        ("localhost", "example.test"),
    ] {
        assert!(
            config(left, right, 8080).is_ok(),
            "distinct endpoints: {left} / {right}"
        );
    }
}

#[test]
fn control_grpc_recognizes_bracketed_ipv6_and_mapped_loopback() {
    for address in ["::1", "[::1]", "0:0:0:0:0:0:0:1", "[::ffff:127.0.0.1]"] {
        let config = RuntimeConfig::parse(&json!({
            "api":{"control":{"enabled":true,"listen":{"address":address,"port":9090},"api_key":"test","grpc":{}}},
            "route":{"final":{"type":"direct"}}
        }).to_string());
        assert!(config.is_ok(), "{address}: {config:?}");
    }
    for address in ["::", "[::]", "[2001:db8::1]"] {
        let config = RuntimeConfig::parse(&json!({
            "api":{"control":{"enabled":true,"listen":{"address":address,"port":9090},"api_key":"test","grpc":{}}},
            "route":{"final":{"type":"direct"}}
        }).to_string());
        assert!(
            matches!(config, Err(ConfigError::InvalidApi(_))),
            "{address}"
        );
    }
}
