use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use zero_config::{
    EventSinkConfig, InboundProtocolConfig, LoadBalanceStrategy, ModeConfig, OutboundGroupKind,
    OutboundProtocolConfig, RouteActionConfig, RuleConditionConfig, RuntimeConfig,
};

#[test]
fn parses_config_into_adts() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "socks-in",
                    "listen": { "address": "127.0.0.1", "port": 1080 },
                    "protocol": { "type": "socks5" }
                },
                {
                    "tag": "http-in",
                    "listen": { "address": "127.0.0.1", "port": 8080 },
                    "protocol": { "type": "http" }
                }
            ],
            "outbounds": [
                {
                    "tag": "direct",
                    "protocol": { "type": "direct" }
                },
                {
                    "tag": "block",
                    "protocol": { "type": "block" }
                },
                {
                    "tag": "chain",
                    "protocol": { "type": "socks5", "server": "127.0.0.1", "port": 2080 }
                }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["chain", "direct"],
                    "selected": "chain"
                }
            ],
            "runtime": {
                "udp_upstream_idle_timeout_seconds": 12
            },
            "mode": {
                "type": "global",
                "outbound": "proxy"
            },
            "route": {
                "rules": [
                    {
                        "condition": {
                            "type": "or",
                            "items": [
                                { "type": "domain", "values": ["blocked.example"] },
                                { "type": "ip", "values": ["10.0.0.0/8"] }
                            ]
                        },
                        "action": { "type": "route", "outbound": "block" }
                    }
                ],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    assert!(matches!(
        config.inbounds[0].protocol,
        InboundProtocolConfig::Socks5 { .. }
    ));
    assert!(matches!(
        config.inbounds[1].protocol,
        InboundProtocolConfig::HttpConnect
    ));
    assert!(matches!(
        config.outbounds[0].protocol,
        OutboundProtocolConfig::Direct
    ));
    assert!(matches!(
        config.outbounds[1].protocol,
        OutboundProtocolConfig::Block
    ));
    assert!(matches!(
        config.outbounds[2].protocol,
        OutboundProtocolConfig::Socks5 { .. }
    ));
    assert!(matches!(
        config.outbound_groups[0].group,
        OutboundGroupKind::Selector { .. }
    ));
    assert_eq!(config.runtime.udp_upstream_idle_timeout_seconds, 12);
    assert!(matches!(config.mode, ModeConfig::Global { .. }));
    assert!(matches!(
        config.route.final_action,
        RouteActionConfig::Direct
    ));
    assert!(matches!(
        config.route.rules[0].condition,
        RuleConditionConfig::Or { .. }
    ));
}

#[test]
fn parses_vless_inbound_and_outbound_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vless-in",
                    "listen": { "address": "127.0.0.1", "port": 1082 },
                    "protocol": {
                        "type": "vless",
                        "users": [
                            {
                                "id": "11111111-2222-3333-4444-555555555555",
                                "principal_key": "user:10001"
                            }
                        ]
                    }
                }
            ],
            "outbounds": [
                {
                    "tag": "vless-chain",
                    "protocol": {
                        "type": "vless",
                        "server": "127.0.0.1",
                        "port": 2081,
                        "id": "11111111-2222-3333-4444-555555555555"
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vless-chain" }
            }
        }"#,
    )
    .expect("config should parse");

    match &config.inbounds[0].protocol {
        InboundProtocolConfig::Vless { users, .. } => {
            assert_eq!(users[0].principal_key.as_deref(), Some("user:10001"));
        }
        _ => panic!("expected vless inbound"),
    }
    assert_eq!(config.inbounds[0].protocol.vless_users().len(), 1);
    assert!(matches!(
        config.outbounds[0].protocol,
        OutboundProtocolConfig::Vless { .. }
    ));
}

#[test]
fn parses_native_vless_mux_and_xudp_concurrency() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "vless-mux",
                    "protocol": {
                        "type": "vless",
                        "server": "127.0.0.1",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "mux_concurrency": 16,
                        "xudp_concurrency": 32,
                        "mux_idle_timeout_secs": 60,
                        "mux_response_backlog_frames": 128,
                        "mux_response_backlog_bytes": 2097152
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vless-mux" }
            }
        }"#,
    )
    .expect("native VLESS MUX/XUDP config should parse");

    match &config.outbounds[0].protocol {
        OutboundProtocolConfig::Vless {
            mux_concurrency,
            xudp_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            ..
        } => {
            assert_eq!(*mux_concurrency, Some(16));
            assert_eq!(*xudp_concurrency, Some(32));
            assert_eq!(*mux_idle_timeout_secs, Some(60));
            assert_eq!(*mux_response_backlog_frames, Some(128));
            assert_eq!(*mux_response_backlog_bytes, Some(2 * 1024 * 1024));
        }
        _ => panic!("expected vless outbound"),
    }
}

#[test]
fn parses_native_mux_response_backlog_policy_for_vless_and_vmess_inbounds() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vless-in",
                    "listen": { "address": "127.0.0.1", "port": 1443 },
                    "protocol": {
                        "type": "vless",
                        "users": [
                            { "id": "11111111-2222-3333-4444-555555555555" }
                        ],
                        "mux_response_backlog_frames": 1,
                        "mux_response_backlog_bytes": 16384
                    }
                },
                {
                    "tag": "vmess-in",
                    "listen": { "address": "127.0.0.1", "port": 2443 },
                    "protocol": {
                        "type": "vmess",
                        "users": [
                            {
                                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                                "cipher": "none"
                            }
                        ],
                        "tls": {
                            "cert_path": "server.crt",
                            "key_path": "server.key"
                        },
                        "mux_response_backlog_frames": 4096,
                        "mux_response_backlog_bytes": 67108864
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("native inbound MUX response backlog policy should parse");

    match &config.inbounds[0].protocol {
        InboundProtocolConfig::Vless {
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            ..
        } => {
            assert_eq!(*mux_response_backlog_frames, Some(1));
            assert_eq!(*mux_response_backlog_bytes, Some(16 * 1024));
        }
        _ => panic!("expected vless inbound"),
    }
    match &config.inbounds[1].protocol {
        InboundProtocolConfig::Vmess {
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            ..
        } => {
            assert_eq!(*mux_response_backlog_frames, Some(4096));
            assert_eq!(*mux_response_backlog_bytes, Some(64 * 1024 * 1024));
        }
        _ => panic!("expected vmess inbound"),
    }
}

#[test]
fn parses_native_vmess_mux_response_backlog_policy() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "vmess-mux",
                    "protocol": {
                        "type": "vmess",
                        "server": "127.0.0.1",
                        "port": 443,
                        "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                        "cipher": "none",
                        "mux_concurrency": 8,
                        "mux_response_backlog_frames": 64,
                        "mux_response_backlog_bytes": 1048576
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vmess-mux" }
            }
        }"#,
    )
    .expect("native VMess MUX response backlog policy should parse");

    match &config.outbounds[0].protocol {
        OutboundProtocolConfig::Vmess {
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            ..
        } => {
            assert_eq!(*mux_response_backlog_frames, Some(64));
            assert_eq!(*mux_response_backlog_bytes, Some(1024 * 1024));
        }
        _ => panic!("expected vmess outbound"),
    }
}

#[test]
fn rejects_native_mux_response_backlog_values_outside_safe_bounds() {
    for protocol in ["vless", "vmess"] {
        for (field, value) in [
            ("mux_response_backlog_frames", 0_u64),
            ("mux_response_backlog_frames", 4097),
            ("mux_response_backlog_bytes", 16383),
            ("mux_response_backlog_bytes", 67_108_865),
        ] {
            let cipher = if protocol == "vmess" {
                r#", "cipher": "none""#
            } else {
                ""
            };
            let config = format!(
                r#"{{
                    "outbounds": [
                        {{
                            "tag": "mux",
                            "protocol": {{
                                "type": "{protocol}",
                                "server": "127.0.0.1",
                                "port": 443,
                                "id": "11111111-2222-3333-4444-555555555555"
                                {cipher},
                                "{field}": {value}
                            }}
                        }}
                    ],
                    "route": {{
                        "rules": [],
                        "final": {{ "type": "route", "outbound": "mux" }}
                    }}
                }}"#
            );
            assert!(
                RuntimeConfig::parse(&config).is_err(),
                "{protocol} {field}={value} should be rejected"
            );
        }
    }
}

#[test]
fn rejects_invalid_native_vless_mux_concurrency() {
    for field in ["mux_concurrency", "xudp_concurrency"] {
        for value in [0_u32, u16::MAX as u32 + 1] {
            let config = format!(
                r#"{{
                    "outbounds": [
                        {{
                            "tag": "vless-mux",
                            "protocol": {{
                                "type": "vless",
                                "server": "127.0.0.1",
                                "port": 443,
                                "id": "11111111-2222-3333-4444-555555555555",
                                "{field}": {value}
                            }}
                        }}
                    ],
                    "route": {{
                        "rules": [],
                        "final": {{ "type": "route", "outbound": "vless-mux" }}
                    }}
                }}"#
            );
            assert!(
                RuntimeConfig::parse(&config).is_err(),
                "{field}={value} should be rejected"
            );
        }
    }
}

#[test]
fn parses_vmess_inbound_and_outbound_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vmess-in",
                    "listen": { "address": "127.0.0.1", "port": 1082 },
                    "protocol": {
                        "type": "vmess",
                        "users": [
                            {
                                "id": "11111111-2222-3333-4444-555555555555",
                                "cipher": "chacha20-poly1305",
                                "principal_key": "user:10001"
                            }
                        ],
                        "tls": {
                            "cert_path": "certs/server.crt",
                            "key_path": "certs/server.key"
                        }
                    }
                }
            ],
            "outbounds": [
                {
                    "tag": "vmess-chain",
                    "protocol": {
                        "type": "vmess",
                        "server": "example.com",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "cipher": "chacha20-poly1305",
                        "tls": {
                            "server_name": "example.com",
                            "ca_cert_path": "certs/ca.pem"
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vmess-chain" }
            }
        }"#,
    )
    .expect("vmess config should parse");

    assert!(matches!(
        config.inbounds[0].protocol,
        InboundProtocolConfig::Vmess { .. }
    ));
    assert!(matches!(
        config.outbounds[0].protocol,
        OutboundProtocolConfig::Vmess { .. }
    ));
}

#[test]
fn rejects_vmess_inbound_without_tls() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vmess-in",
                    "listen": { "address": "127.0.0.1", "port": 1082 },
                    "protocol": {
                        "type": "vmess",
                        "users": [
                            { "id": "11111111-2222-3333-4444-555555555555" }
                        ]
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("vmess inbound without tls should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidInbound(_)));
}

#[test]
fn normalizes_vmess_cipher_auto_to_aead_baseline() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vmess-in",
                    "listen": { "address": "127.0.0.1", "port": 1082 },
                    "protocol": {
                        "type": "vmess",
                        "users": [
                            {
                                "id": "11111111-2222-3333-4444-555555555555",
                                "cipher": "auto"
                            }
                        ],
                        "tls": {
                            "cert_path": "certs/server.crt",
                            "key_path": "certs/server.key"
                        }
                    }
                }
            ],
            "outbounds": [
                {
                    "tag": "vmess-chain",
                    "protocol": {
                        "type": "vmess",
                        "server": "example.com",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "cipher": "auto"
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vmess-chain" }
            }
        }"#,
    )
    .expect("vmess cipher auto should normalize");

    match &config.inbounds[0].protocol {
        InboundProtocolConfig::Vmess { users, .. } => {
            assert_eq!(users[0].cipher, "aes-128-gcm");
        }
        _ => panic!("expected vmess inbound"),
    }

    match &config.outbounds[0].protocol {
        OutboundProtocolConfig::Vmess { cipher, .. } => {
            assert_eq!(cipher, "aes-128-gcm");
        }
        _ => panic!("expected vmess outbound"),
    }
}

#[test]
fn rejects_unknown_vmess_cipher() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "vmess-chain",
                    "protocol": {
                        "type": "vmess",
                        "server": "example.com",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "cipher": "bogus"
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vmess-chain" }
            }
        }"#,
    )
    .expect_err("unsupported vmess cipher should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::InvalidOutbound(_)
    ));
}

#[test]
fn rejects_vmess_ws_and_grpc_together() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vmess-in",
                    "listen": { "address": "127.0.0.1", "port": 1082 },
                    "protocol": {
                        "type": "vmess",
                        "users": [
                            { "id": "11111111-2222-3333-4444-555555555555" }
                        ],
                        "tls": {
                            "cert_path": "certs/server.crt",
                            "key_path": "certs/server.key"
                        },
                        "ws": { "path": "/vmess" },
                        "grpc": { "service_names": ["zero.vmess"] }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("vmess inbound ws and grpc together should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidInbound(_)));
}

#[test]
fn rejects_grpc_transport_without_service_names() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "vless-grpc-chain",
                    "protocol": {
                        "type": "vless",
                        "server": "example.com",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "grpc": {}
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vless-grpc-chain" }
            }
        }"#,
    )
    .expect_err("grpc transport without service_names should fail");

    assert!(matches!(error, zero_config::ConfigError::ParseConfig(_)));
    assert!(error.to_string().contains("service_names"));
}

#[test]
fn parses_vless_tls_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vless-tls-in",
                    "listen": { "address": "127.0.0.1", "port": 8443 },
                    "protocol": {
                        "type": "vless",
                        "users": [
                            { "id": "11111111-2222-3333-4444-555555555555" }
                        ],
                        "tls": {
                            "cert_path": "certs/fullchain.pem",
                            "key_path": "certs/privkey.pem"
                        }
                    }
                }
            ],
            "outbounds": [
                {
                    "tag": "vless-tls-chain",
                    "protocol": {
                        "type": "vless",
                        "server": "example.com",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "tls": {
                            "server_name": "edge.example.com",
                            "ca_cert_path": "certs/ca.pem"
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vless-tls-chain" }
            }
        }"#,
    )
    .expect("config should parse");

    let inbound_tls = config.inbounds[0]
        .protocol
        .vless_tls()
        .expect("vless inbound tls");
    assert_eq!(inbound_tls.cert_path, "certs/fullchain.pem");
    assert_eq!(inbound_tls.key_path, "certs/privkey.pem");

    match &config.outbounds[0].protocol {
        OutboundProtocolConfig::Vless { tls, .. } => {
            let tls = tls.as_ref().expect("vless outbound tls");
            assert_eq!(tls.server_name.as_deref(), Some("edge.example.com"));
            assert_eq!(tls.ca_cert_path.as_deref(), Some("certs/ca.pem"));
        }
        _ => panic!("expected vless outbound"),
    }
}

#[test]
fn parses_vless_reality_outbound_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "vless-reality-chain",
                    "protocol": {
                        "type": "vless",
                        "server": "edge.example.com",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "reality": {
                            "public_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "short_id": "0123456789abcdef",
                            "server_name": "www.cloudflare.com",
                            "client_fingerprint": "firefox",
                            "cipher_suites": [
                                "TLS_AES_128_GCM_SHA256",
                                "TLS_CHACHA20_POLY1305_SHA256"
                            ]
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vless-reality-chain" }
            }
        }"#,
    )
    .expect("config should parse");

    match &config.outbounds[0].protocol {
        OutboundProtocolConfig::Vless { reality, .. } => {
            let reality = reality.as_ref().expect("vless outbound reality");
            assert_eq!(reality.server_name.as_deref(), Some("www.cloudflare.com"));
            assert_eq!(reality.short_id, "0123456789abcdef");
            assert_eq!(reality.cipher_suites.len(), 2);
            assert_eq!(reality.client_fingerprint, "firefox");
        }
        _ => panic!("expected vless outbound"),
    }
}

#[test]
fn rejects_unknown_vless_reality_client_fingerprint() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [{
                "tag": "vless-reality-chain",
                "protocol": {
                    "type": "vless",
                    "server": "edge.example.com",
                    "port": 443,
                    "id": "11111111-2222-3333-4444-555555555555",
                    "reality": {
                        "public_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        "client_fingerprint": "netscape"
                    }
                }
            }],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vless-reality-chain" }
            }
        }"#,
    )
    .expect_err("unknown fingerprint should fail");

    assert!(error.to_string().contains("unsupported client fingerprint"));
}

#[test]
fn parses_vless_reality_inbound_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vless-reality-in",
                    "listen": { "address": "127.0.0.1", "port": 8443 },
                    "protocol": {
                        "type": "vless",
                        "users": [
                            { "id": "11111111-2222-3333-4444-555555555555" }
                        ],
                        "reality": {
                            "private_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                            "short_ids": ["0123456789abcdef"],
                            "server_name": "www.cloudflare.com",
                            "cipher_suites": ["TLS_AES_128_GCM_SHA256"]
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    let reality = config.inbounds[0]
        .protocol
        .vless_reality()
        .expect("vless inbound reality");
    assert_eq!(reality.short_ids, vec!["0123456789abcdef"]);
    assert_eq!(reality.server_name.as_deref(), Some("www.cloudflare.com"));
}

#[test]
fn rejects_invalid_vless_reality_config() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "vless-reality-chain",
                    "protocol": {
                        "type": "vless",
                        "server": "edge.example.com",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "reality": {
                            "public_key": "bad",
                            "short_id": "0123456789abcdef00"
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vless-reality-chain" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::InvalidOutbound(_)
    ));
}

#[test]
fn rejects_invalid_vless_inbound_reality_config() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vless-reality-in",
                    "listen": { "address": "127.0.0.1", "port": 8443 },
                    "protocol": {
                        "type": "vless",
                        "users": [
                            { "id": "11111111-2222-3333-4444-555555555555" }
                        ],
                        "reality": {
                            "private_key": "invalid",
                            "short_ids": ["not-hex"]
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidInbound(_)));
}

#[test]
fn rejects_vless_reality_with_tls_or_ws() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "vless-reality-chain",
                    "protocol": {
                        "type": "vless",
                        "server": "edge.example.com",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "tls": {},
                        "reality": {
                            "public_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vless-reality-chain" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::InvalidOutbound(_)
    ));

    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "vless-reality-chain",
                    "protocol": {
                        "type": "vless",
                        "server": "edge.example.com",
                        "port": 443,
                        "id": "11111111-2222-3333-4444-555555555555",
                        "ws": { "path": "/vless" },
                        "reality": {
                            "public_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "vless-reality-chain" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::InvalidOutbound(_)
    ));
}

#[test]
fn rejects_vless_inbound_reality_with_tls_or_ws() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vless-reality-in",
                    "listen": { "address": "127.0.0.1", "port": 8443 },
                    "protocol": {
                        "type": "vless",
                        "users": [
                            { "id": "11111111-2222-3333-4444-555555555555" }
                        ],
                        "tls": {
                            "cert_path": "cert.pem",
                            "key_path": "key.pem"
                        },
                        "reality": {
                            "private_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidInbound(_)));

    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vless-reality-in",
                    "listen": { "address": "127.0.0.1", "port": 8443 },
                    "protocol": {
                        "type": "vless",
                        "users": [
                            { "id": "11111111-2222-3333-4444-555555555555" }
                        ],
                        "ws": { "path": "/vless" },
                        "reality": {
                            "private_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidInbound(_)));
}

#[test]
fn rejects_empty_vless_tls_paths() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vless-tls-in",
                    "listen": { "address": "127.0.0.1", "port": 8443 },
                    "protocol": {
                        "type": "vless",
                        "users": [
                            { "id": "11111111-2222-3333-4444-555555555555" }
                        ],
                        "tls": {
                            "cert_path": "",
                            "key_path": "certs/privkey.pem"
                        }
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidInbound(_)));
}

#[test]
fn rejects_invalid_vless_uuid() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "vless-in",
                    "listen": { "address": "127.0.0.1", "port": 1082 },
                    "protocol": {
                        "type": "vless",
                        "users": [
                            { "id": "not-a-uuid" }
                        ]
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidInbound(_)));
}

#[test]
fn accepts_all_shadowsocks_supported_ciphers() {
    const CIPHERS: &[&str] = &[
        "aes-128-gcm",
        "aes-256-gcm",
        "chacha20-ietf-poly1305",
        "2022-blake3-aes-128-gcm",
        "2022-blake3-aes-256-gcm",
        "2022-blake3-chacha20-poly1305",
    ];

    for cipher in CIPHERS {
        let password = shadowsocks_password_for_cipher(cipher);
        let config = RuntimeConfig::parse(&format!(
            r#"{{
                "inbounds": [
                    {{
                        "tag": "ss-in",
                        "listen": {{ "address": "127.0.0.1", "port": 8388 }},
                        "protocol": {{
                            "type": "shadowsocks",
                            "password": "{password}",
                            "cipher": "{cipher}"
                        }}
                    }}
                ],
                "outbounds": [
                    {{
                        "tag": "ss-out",
                        "protocol": {{
                            "type": "shadowsocks",
                            "server": "127.0.0.1",
                            "port": 8389,
                            "password": "{password}",
                            "cipher": "{cipher}"
                        }}
                    }}
                ],
                "route": {{
                    "rules": [],
                    "final": {{ "type": "route", "outbound": "ss-out" }}
                }}
            }}"#
        ))
        .expect("shadowsocks cipher should parse");

        match &config.inbounds[0].protocol {
            InboundProtocolConfig::Shadowsocks { cipher: parsed, .. } => assert_eq!(parsed, cipher),
            _ => panic!("expected shadowsocks inbound"),
        }
        match &config.outbounds[0].protocol {
            OutboundProtocolConfig::Shadowsocks { cipher: parsed, .. } => {
                assert_eq!(parsed, cipher)
            }
            _ => panic!("expected shadowsocks outbound"),
        }
    }
}

fn shadowsocks_password_for_cipher(cipher: &str) -> &'static str {
    match cipher {
        "2022-blake3-aes-128-gcm" => "MDEyMzQ1Njc4OWFiY2RlZg==",
        "2022-blake3-aes-256-gcm" | "2022-blake3-chacha20-poly1305" => {
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
        }
        _ => "secret",
    }
}

#[test]
fn rejects_invalid_shadowsocks_cipher_and_empty_outbound_password() {
    let cipher_error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "ss-in",
                    "listen": { "address": "127.0.0.1", "port": 8388 },
                    "protocol": {
                        "type": "shadowsocks",
                        "password": "secret",
                        "cipher": "unsupported"
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("invalid shadowsocks cipher should fail");

    assert!(matches!(
        cipher_error,
        zero_config::ConfigError::InvalidInbound(_)
    ));

    let password_error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "ss-out",
                    "protocol": {
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": 8389,
                        "password": ""
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "ss-out" }
            }
        }"#,
    )
    .expect_err("empty shadowsocks outbound password should fail");

    assert!(matches!(
        password_error,
        zero_config::ConfigError::InvalidOutbound(_)
    ));
}

#[test]
fn rejects_invalid_shadowsocks_2022_password_key_material() {
    let inbound_error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "ss-in",
                    "listen": { "address": "127.0.0.1", "port": 8388 },
                    "protocol": {
                        "type": "shadowsocks",
                        "password": "secret",
                        "cipher": "2022-blake3-aes-128-gcm"
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("invalid shadowsocks 2022 password should fail");

    assert!(matches!(
        inbound_error,
        zero_config::ConfigError::InvalidInbound(_)
    ));

    let outbound_error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "ss-out",
                    "protocol": {
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": 8389,
                        "password": "MDEyMzQ1Njc4OWFiY2RlZg==",
                        "cipher": "2022-blake3-aes-256-gcm"
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "ss-out" }
            }
        }"#,
    )
    .expect_err("wrong shadowsocks 2022 password length should fail");

    assert!(matches!(
        outbound_error,
        zero_config::ConfigError::InvalidOutbound(_)
    ));
}

#[test]
fn parses_api_event_sinks_and_control_config() {
    let config = RuntimeConfig::parse(
        r#"{
            "api": {
                "event_sinks": [
                    {
                        "tag": "receiver",
                        "type": "webhook",
                        "url": "https://receiver.example.com/hooks/zero",
                        "events": ["flow.completed", "engine.warning"],
                        "source_id": "edge-01",
                        "headers": {
                            "authorization": "Bearer receiver-token"
                        }
                    },
                    {
                        "tag": "local-events",
                        "type": "jsonl",
                        "path": "zero-events.jsonl",
                        "events": ["flow.completed"]
                    }
                ],
                "control": {
                    "enabled": true,
                    "listen": { "address": "127.0.0.1", "port": 9090 },
                    "api_key_env": "ZERO_NODE_API_KEY",
                    "grpc": {
                        "bearer_auth": false,
                        "tls": {
                            "cert_path": "managed/grpc/server.pem",
                            "key_path": "managed/grpc/server-key.pem",
                            "client_ca_cert_path": "managed/grpc/client-ca.pem"
                        }
                    }
                },
                "dispatcher": {
                    "max_in_memory_deliveries": 128,
                    "replay_batch_size": 256,
                    "max_retry_attempts": 5,
                    "retry_initial_delay_ms": 250,
                    "retry_max_delay_ms": 8000,
                    "webhook_timeout_ms": 3000,
                    "outbox_min_free_bytes": 2147483648,
                    "outbox_min_free_percent": 8,
                    "exhausted_delivery_policy": "discard"
                }
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    assert_eq!(config.api.event_sinks.len(), 2);
    let EventSinkConfig::Webhook {
        tag,
        url,
        events,
        source_id,
        headers,
        ..
    } = &config.api.event_sinks[0]
    else {
        panic!("expected webhook sink");
    };
    assert_eq!(tag, "receiver");
    assert_eq!(url, "https://receiver.example.com/hooks/zero");
    assert_eq!(events, &["flow.completed", "engine.warning"]);
    assert_eq!(source_id.as_deref(), Some("edge-01"));
    assert_eq!(
        headers.get("authorization").map(String::as_str),
        Some("Bearer receiver-token")
    );

    assert!(config.api.control.enabled);
    assert_eq!(config.api.dispatcher.max_in_memory_deliveries, 128);
    assert_eq!(config.api.dispatcher.replay_batch_size, 256);
    assert_eq!(config.api.dispatcher.max_retry_attempts, 5);
    assert_eq!(config.api.dispatcher.retry_initial_delay_ms, 250);
    assert_eq!(config.api.dispatcher.retry_max_delay_ms, 8_000);
    assert_eq!(config.api.dispatcher.webhook_timeout_ms, 3_000);
    assert_eq!(
        config.api.dispatcher.outbox_min_free_bytes,
        2 * 1024 * 1024 * 1024
    );
    assert_eq!(config.api.dispatcher.outbox_min_free_percent, 8);
    assert_eq!(
        config.api.dispatcher.exhausted_delivery_policy,
        zero_config::ExhaustedDeliveryPolicy::Discard
    );
    assert_eq!(
        config.api.control.listen.as_ref().expect("listen").port,
        9090
    );
    let grpc = config.api.control.grpc.as_ref().expect("gRPC policy");
    assert!(!grpc.bearer_auth);
    let grpc_tls = grpc.tls.as_ref().expect("gRPC TLS");
    assert_eq!(grpc_tls.cert_path, "managed/grpc/server.pem");
    assert_eq!(
        grpc_tls.client_ca_cert_path.as_deref(),
        Some("managed/grpc/client-ca.pem")
    );
}

#[test]
fn remote_grpc_plaintext_requires_explicit_opt_in() {
    let error = RuntimeConfig::parse(
        r#"{
            "api": {
                "control": {
                    "enabled": true,
                    "listen": { "address": "0.0.0.0", "port": 9090 },
                    "api_key": "secret",
                    "grpc": {}
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect_err("remote plaintext must require opt-in");
    assert!(
        error.to_string().contains("allow_insecure_remote"),
        "{error}"
    );

    RuntimeConfig::parse(
        r#"{
            "api": {
                "control": {
                    "enabled": true,
                    "listen": { "address": "0.0.0.0", "port": 9090 },
                    "api_key": "secret",
                    "grpc": { "allow_insecure_remote": true }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("explicit remote plaintext");
}

#[test]
fn remote_http_control_does_not_require_a_grpc_policy() {
    RuntimeConfig::parse(
        r#"{
            "api": {
                "control": {
                    "enabled": true,
                    "listen": { "address": "0.0.0.0", "port": 9090 },
                    "api_key": "secret"
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("HTTP-only builds must not be constrained by an absent gRPC policy");
}

#[test]
fn remote_grpc_without_bearer_requires_mtls() {
    let error = RuntimeConfig::parse(
        r#"{
            "api": {
                "control": {
                    "enabled": true,
                    "listen": { "address": "0.0.0.0", "port": 9090 },
                    "api_key": "http-secret",
                    "grpc": {
                        "bearer_auth": false,
                        "tls": {
                            "cert_path": "server.pem",
                            "key_path": "server-key.pem"
                        }
                    }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect_err("server-only TLS still needs caller authentication");
    assert!(error.to_string().contains("requires mTLS"), "{error}");

    RuntimeConfig::parse(
        r#"{
            "api": {
                "control": {
                    "enabled": true,
                    "listen": { "address": "0.0.0.0", "port": 9090 },
                    "api_key": "http-secret",
                    "grpc": {
                        "bearer_auth": false,
                        "tls": {
                            "cert_path": "server.pem",
                            "key_path": "server-key.pem",
                            "client_ca_cert_path": "client-ca.pem"
                        }
                    }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("mTLS provides remote caller authentication");
}

#[test]
fn native_grpc_tls_rejects_insecure_remote_override() {
    let error = RuntimeConfig::parse(
        r#"{
            "api": {
                "control": {
                    "enabled": true,
                    "listen": { "address": "0.0.0.0", "port": 9090 },
                    "api_key": "secret",
                    "grpc": {
                        "allow_insecure_remote": true,
                        "tls": {
                            "cert_path": "server.pem",
                            "key_path": "server-key.pem"
                        }
                    }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect_err("native TLS and insecure override are contradictory");
    assert!(error.to_string().contains("must be false"), "{error}");
}

#[test]
fn outbox_disk_reserve_requires_safe_non_zero_values() {
    for (field, value) in [
        ("outbox_min_free_bytes", serde_json::json!(0)),
        ("outbox_min_free_percent", serde_json::json!(0)),
        ("outbox_min_free_percent", serde_json::json!(51)),
    ] {
        let mut dispatcher = serde_json::Map::new();
        dispatcher.insert(field.to_owned(), value);
        let raw = serde_json::json!({
            "api": { "dispatcher": dispatcher },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        })
        .to_string();
        let error = RuntimeConfig::parse(&raw).expect_err("unsafe disk reserve must fail");
        assert!(error.to_string().contains(field), "{error}");
    }
}

#[test]
fn zero_memory_delivery_workset_requires_an_outbox() {
    let error = RuntimeConfig::parse(
        r#"{
            "api": {
                "dispatcher": { "max_in_memory_deliveries": 0 }
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("zero memory workset without an outbox should fail");
    assert!(error.to_string().contains("requires api.outbox_path"));
}

#[test]
fn zero_memory_delivery_workset_is_valid_with_an_outbox() {
    RuntimeConfig::parse(
        r#"{
            "api": {
                "outbox_path": "events.outbox",
                "dispatcher": { "max_in_memory_deliveries": 0 }
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("zero memory workset with an outbox should be valid");
}

#[test]
fn dead_letter_exhaustion_policy_requires_a_dead_letter_path() {
    let error = RuntimeConfig::parse(
        r#"{
            "api": {
                "dispatcher": {
                    "exhausted_delivery_policy": "dead_letter"
                }
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("dead-letter policy without storage should fail");

    assert!(error.to_string().contains("requires api.dead_letter_path"));
}

#[test]
fn rejects_unknown_api_event_type() {
    let error = RuntimeConfig::parse(
        r#"{
            "api": {
                "event_sinks": [
                    {
                        "tag": "receiver",
                        "type": "webhook",
                        "url": "https://receiver.example.com/hooks/zero",
                        "events": ["receiver.user.changed"]
                    }
                ]
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("unknown event type should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidApi(_)));
}

#[test]
fn rejects_insecure_webhook_without_explicit_opt_in() {
    let error = RuntimeConfig::parse(
        r#"{
            "api": {
                "event_sinks": [
                    {
                        "tag": "receiver",
                        "type": "webhook",
                        "url": "http://127.0.0.1:9000/events",
                        "events": ["flow.completed"]
                    }
                ]
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("http webhook should require allow_insecure");

    assert!(matches!(error, zero_config::ConfigError::InvalidApi(_)));
}

#[test]
fn runtime_idle_timeout_defaults_to_thirty_seconds() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    assert_eq!(config.runtime.udp_upstream_idle_timeout_seconds, 30);
    assert_eq!(config.runtime.event_log_capacity, 1024);
    assert!(config.runtime.udp.enabled);
}

#[test]
fn parses_event_log_capacity_override() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "event_log_capacity": 2048
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    assert_eq!(config.runtime.event_log_capacity, 2048);
}

#[test]
fn parses_udp_policy_overrides() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": { "udp": { "enabled": false } },
            "inbounds": [
                {
                    "tag": "socks-in",
                    "listen": { "address": "127.0.0.1", "port": 1080 },
                    "udp": { "enabled": false },
                    "protocol": { "type": "socks5" }
                }
            ],
            "outbounds": [
                {
                    "tag": "direct",
                    "udp": { "enabled": false },
                    "protocol": { "type": "direct" }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    assert!(!config.runtime.udp.enabled);
    assert!(!config.inbounds[0].udp.enabled);
    assert!(!config.outbounds[0].udp.enabled);
}

#[test]
fn rejects_zero_udp_upstream_idle_timeout() {
    let error = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "udp_upstream_idle_timeout_seconds": 0
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidRuntime(_)));
}

#[test]
fn rejects_zero_event_log_capacity() {
    let error = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "event_log_capacity": 0
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidRuntime(_)));
}

#[test]
fn parses_and_validates_principal_quota_recovery_path() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": { "principal_quota_state_path": "state/principal-quotas.json" },
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("valid quota recovery path");
    assert_eq!(
        config.runtime.principal_quota_state_path.as_deref(),
        Some("state/principal-quotas.json")
    );

    let error = RuntimeConfig::parse(
        r#"{
            "runtime": { "principal_quota_state_path": "  " },
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect_err("empty quota recovery path must fail");
    assert!(error.to_string().contains("principal_quota_state_path"));
}

#[test]
fn parses_global_latency_url_and_network_mtu() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "latency_test_url": "http://probe.example/generate_204",
                "network": { "mtu": 1400 }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("config should parse");

    assert_eq!(
        config.runtime.effective_latency_test_url(),
        "http://probe.example/generate_204"
    );
    assert_eq!(
        config
            .runtime
            .latency_test_url_or(Some("http://command.example/")),
        "http://probe.example/generate_204"
    );
    assert_eq!(config.runtime.network.mtu, 1400);
}

#[test]
fn rejects_invalid_global_latency_url_and_network_mtu() {
    for runtime in [
        r#"{ "latency_test_url": "https://probe.example/" }"#,
        r#"{ "network": { "mtu": 575 } }"#,
    ] {
        let raw = format!(
            r#"{{
                "runtime": {runtime},
                "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
            }}"#
        );
        let error = RuntimeConfig::parse(&raw).expect_err("config should fail");
        assert!(matches!(error, zero_config::ConfigError::InvalidRuntime(_)));
    }
}

#[test]
fn parses_declarative_tun_runtime_with_safe_defaults() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "network": { "mtu": 1400 },
                "dns": {
                    "servers": { "global": { "type": "udp", "host": "1.1.1.1" } },
                    "default_server": "global"
                },
                "tun": { "addr": "10.23.0.1/24" }
            },
            "route": {
                "rules": [{
                    "condition": { "type": "inbound", "values": ["tun"] },
                    "action": { "type": "direct" }
                }],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("declarative TUN config should parse");

    let tun = config.runtime.tun.expect("TUN should be enabled");
    assert_eq!(tun.addr, "10.23.0.1/24");
    assert_eq!(tun.secondary_addr, None);
    assert_eq!(tun.tag, "tun");
    assert_eq!(tun.effective_mtu(config.runtime.network.mtu), 1400);
    assert!(tun.auto_route);
    assert!(tun.include_cidrs.is_empty());
    assert!(tun.exclude_cidrs.is_empty());
    assert!(tun.dual_stack);
    assert!(tun.strict_route);
    assert!(tun.dns_hijack);
}

#[test]
fn strict_tun_dns_hijack_accepts_system_dns_for_runtime_discovery() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": { "system-auto": { "type": "system" } },
                    "default_server": "system-auto"
                },
                "tun": { "addr": "10.23.0.1/24" }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("system DNS endpoints are resolved during TUN route preparation");

    let dns = config.runtime.dns.expect("DNS config");
    assert!(dns.uses_system_dns());
    assert!(dns.tun_route_exclusion_addresses().unwrap().is_empty());
}

#[test]
fn validates_declarative_tun_capture_cidrs() {
    for tun in [
        r#"{ "addr": "10.0.0.1/24", "auto_route": false, "include_cidrs": ["10.0.0.0/8"] }"#,
        r#"{ "addr": "10.0.0.1/24", "dual_stack": false, "include_cidrs": ["2001:db8::/32"] }"#,
        r#"{ "addr": "10.0.0.1/24", "include_cidrs": ["10.0.0.0/8", "10.0.0.0/8"] }"#,
        r#"{ "addr": "10.0.0.1/24", "auto_route": false, "exclude_cidrs": ["192.168.0.0/16"] }"#,
        r#"{ "addr": "10.0.0.1/24", "dual_stack": false, "exclude_cidrs": ["2001:db8::/32"] }"#,
        r#"{ "addr": "10.0.0.1/24", "exclude_cidrs": ["192.168.0.0/16", "192.168.0.0/16"] }"#,
    ] {
        let raw = format!(
            r#"{{
                "runtime": {{
                    "dns": {{
                        "servers": {{ "global": {{ "type": "udp", "host": "1.1.1.1" }} }},
                        "default_server": "global"
                    }},
                    "tun": {tun}
                }},
                "route": {{ "final": {{ "type": "direct" }} }}
            }}"#
        );
        assert!(matches!(
            RuntimeConfig::parse(&raw),
            Err(zero_config::ConfigError::InvalidRuntime(_))
        ));
    }
}

#[test]
fn validates_declarative_tun_address_mask_mtu_and_tag() {
    for tun in [
        r#"{ "addr": "bad" }"#,
        r#"{ "addr": "10.0.0.1/33" }"#,
        r#"{ "addr": "10.0.0.1", "mask": "ffff:ffff:ffff:ffff::" }"#,
        r#"{ "addr": "10.0.0.1", "mask": "255.0.255.0" }"#,
        r#"{ "addr": "10.0.0.1/24", "mtu": 575 }"#,
        r#"{ "addr": "10.0.0.1/24", "tag": "" }"#,
        r#"{ "addr": "10.0.0.1/24", "name": "" }"#,
        r#"{ "addr": "10.0.0.1/24", "secondary_addr": "fd66::1" }"#,
        r#"{ "addr": "10.0.0.1/24", "secondary_addr": "10.1.0.1/24" }"#,
        r#"{ "addr": "10.0.0.1/24", "secondary_addr": "fd66::1/129" }"#,
        r#"{ "addr": "10.0.0.1/24", "secondary_addr": "fd66::1/64", "dual_stack": false }"#,
    ] {
        let raw = format!(
            r#"{{
                "runtime": {{
                    "dns": {{
                        "servers": {{ "global": {{ "type": "udp", "host": "1.1.1.1" }} }},
                        "default_server": "global"
                    }},
                    "tun": {tun}
                }},
                "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
            }}"#
        );
        let error = RuntimeConfig::parse(&raw).expect_err("invalid TUN config must fail");
        assert!(matches!(error, zero_config::ConfigError::InvalidRuntime(_)));
    }
}

#[test]
fn rejects_tun_tag_that_duplicates_a_listener_tag() {
    let error = RuntimeConfig::parse(
        r#"{
            "runtime": { "tun": { "addr": "10.0.0.1/24", "tag": "entry" } },
            "inbounds": [{
                "tag": "entry",
                "listen": { "address": "127.0.0.1", "port": 1080 },
                "protocol": { "type": "socks5" }
            }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect_err("TUN and listener tags share the inbound namespace");

    assert!(matches!(error, zero_config::ConfigError::InvalidRuntime(_)));
}

#[test]
fn strict_declarative_tun_rejects_recursive_or_hostname_dns_endpoints() {
    for dns in [
        r#"{ "servers": {}, "default_server": "missing" }"#,
        r#"{ "servers": { "local": { "type": "system" } }, "default_server": "local" }"#,
        r#"{ "servers": { "named": { "type": "udp", "host": "dns.example" } }, "default_server": "named" }"#,
        r#"{ "servers": { "named": { "type": "doh", "host": "dns.example" } }, "default_server": "named" }"#,
        r#"{ "servers": { "named": { "type": "dot", "host": "dns.example" } }, "default_server": "named" }"#,
        r#"{ "servers": { "named": { "type": "doq", "host": "dns.example" } }, "default_server": "named" }"#,
    ] {
        let raw = format!(
            r#"{{
                "runtime": {{
                    "dns": {dns},
                    "tun": {{ "addr": "10.0.0.1/24" }}
                }},
                "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
            }}"#
        );
        let error = RuntimeConfig::parse(&raw).expect_err("unsafe TUN DNS must fail early");
        assert!(matches!(
            error,
            zero_config::ConfigError::InvalidRuntime(_) | zero_config::ConfigError::InvalidDns(_)
        ));
    }
}

#[test]
fn parses_uniform_encrypted_dns_endpoints() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": {
                        "https": {
                            "type": "doh",
                            "host": "cloudflare-dns.com",
                            "path": "/dns-query",
                            "bootstrap": ["1.1.1.1", "1.0.0.1"]
                        },
                        "tls": {
                            "type": "dot",
                            "host": "dns.google",
                            "bootstrap": ["8.8.8.8", "8.8.4.4"]
                        },
                        "quic": {
                            "type": "doq",
                            "host": "dns.adguard-dns.com",
                            "bootstrap": ["94.140.14.14", "94.140.15.15"]
                        }
                    },
                    "default_server": "https"
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("encrypted DNS endpoints should parse");

    let dns = config.runtime.dns.expect("DNS config");
    assert_eq!(dns.servers["https"].host(), Some("cloudflare-dns.com"));
    assert_eq!(dns.servers["tls"].port(), Some(853));
    assert_eq!(dns.servers["quic"].port(), Some(853));
}

#[test]
fn parses_dns_timeout_fallback_and_address_family_policy() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": {
                        "primary": { "type": "udp", "host": "1.1.1.1" },
                        "secondary": { "type": "udp", "host": "8.8.8.8" }
                    },
                    "default_server": "primary",
                    "policy": {
                        "timeout_ms": 750,
                        "server_timeout_ms": { "primary": 250 },
                        "fallback_servers": ["secondary"],
                        "node_server": "secondary",
                        "direct_server": "primary",
                        "reject_address_cidrs": ["203.0.113.0/24"],
                        "address_family": "prefer_ipv6"
                    }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("DNS policy should parse");

    let policy = &config.runtime.dns.expect("DNS config").policy;
    assert_eq!(policy.timeout_ms, 750);
    assert_eq!(policy.server_timeout_ms["primary"], 250);
    assert_eq!(policy.fallback_servers, ["secondary"]);
    assert_eq!(policy.node_server.as_deref(), Some("secondary"));
    assert_eq!(policy.direct_server.as_deref(), Some("primary"));
    assert_eq!(policy.reject_address_cidrs[0].to_string(), "203.0.113.0/24");
    assert_eq!(
        policy.address_family,
        zero_config::DnsAddressFamilyPolicy::PreferIpv6
    );
}

#[test]
fn parses_dns_server_detour_with_isolated_node_resolver() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": {
                        "bootstrap": { "type": "udp", "host": "1.1.1.1" },
                        "proxy": {
                            "type": "doh",
                            "host": "dns.example",
                            "bootstrap": ["192.0.2.53"],
                            "detour": "dns-out"
                        }
                    },
                    "default_server": "proxy",
                    "policy": { "node_server": "bootstrap" }
                }
            },
            "outbounds": [{ "tag": "dns-out", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("DNS detour should parse");

    let dns = config.runtime.dns.expect("DNS config");
    assert_eq!(dns.servers["proxy"].detour(), Some("dns-out"));
    assert_eq!(dns.policy.node_server.as_deref(), Some("bootstrap"));
    assert_eq!(
        dns.tun_route_exclusion_addresses().unwrap(),
        vec!["1.1.1.1".parse::<std::net::IpAddr>().unwrap()]
    );
}

#[test]
fn rejects_recursive_or_unsupported_dns_detours() {
    for (servers, policy) in [
        (
            r#"{
                "bootstrap": { "type": "udp", "host": "1.1.1.1" },
                "proxy": { "type": "udp", "host": "8.8.8.8", "detour": "missing" }
            }"#,
            r#"{ "node_server": "bootstrap" }"#,
        ),
        (
            r#"{
                "bootstrap": { "type": "udp", "host": "1.1.1.1" },
                "proxy": { "type": "udp", "host": "8.8.8.8", "detour": "dns-out" }
            }"#,
            r#"{}"#,
        ),
        (
            r#"{
                "bootstrap": { "type": "udp", "host": "1.1.1.1", "detour": "dns-out" },
                "proxy": { "type": "udp", "host": "8.8.8.8", "detour": "dns-out" }
            }"#,
            r#"{ "node_server": "bootstrap" }"#,
        ),
        (
            r#"{
                "bootstrap": { "type": "udp", "host": "1.1.1.1" },
                "proxy": { "type": "doq", "host": "8.8.8.8", "detour": "dns-out" }
            }"#,
            r#"{ "node_server": "bootstrap" }"#,
        ),
    ] {
        let raw = format!(
            r#"{{
                "runtime": {{
                    "dns": {{
                        "servers": {servers},
                        "default_server": "proxy",
                        "policy": {policy}
                    }}
                }},
                "outbounds": [{{ "tag": "dns-out", "protocol": {{ "type": "direct" }} }}],
                "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
            }}"#
        );
        let error = RuntimeConfig::parse(&raw).expect_err("unsafe DNS detour must fail");
        assert!(matches!(error, zero_config::ConfigError::InvalidDns(_)));
    }
}

#[test]
fn rejects_invalid_dns_fallback_policy() {
    for policy in [
        r#"{ "timeout_ms": 0 }"#,
        r#"{ "timeout_ms": 120001 }"#,
        r#"{ "fallback_servers": ["missing"] }"#,
        r#"{ "fallback_servers": ["secondary", "secondary"] }"#,
        r#"{ "server_timeout_ms": { "missing": 100 } }"#,
        r#"{ "server_timeout_ms": { "primary": 0 } }"#,
        r#"{ "node_fallback_servers": ["secondary"] }"#,
        r#"{ "direct_server": "missing" }"#,
    ] {
        let raw = format!(
            r#"{{
                "runtime": {{
                    "dns": {{
                        "servers": {{
                            "primary": {{ "type": "udp", "host": "1.1.1.1" }},
                            "secondary": {{ "type": "udp", "host": "8.8.8.8" }}
                        }},
                        "default_server": "primary",
                        "policy": {policy}
                    }}
                }},
                "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
            }}"#
        );
        let error = RuntimeConfig::parse(&raw).expect_err("invalid DNS policy must fail");
        assert!(matches!(error, zero_config::ConfigError::InvalidDns(_)));
    }
}

#[test]
fn parses_and_validates_dns_real_ip_reverse_mapping() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": { "primary": { "type": "udp", "host": "1.1.1.1" } },
                    "default_server": "primary",
                    "reverse_mapping": {
                        "max_entries": 2048,
                        "max_domains_per_address": 6,
                        "max_ttl_seconds": 180
                    }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("real-IP reverse mapping should parse");
    let reverse = config
        .runtime
        .dns
        .expect("DNS config")
        .reverse_mapping
        .expect("reverse mapping config");
    assert_eq!(reverse.max_entries, 2048);
    assert_eq!(reverse.max_domains_per_address, 6);
    assert_eq!(reverse.max_ttl_seconds, 180);

    for reverse_mapping in [
        r#"{ "max_entries": 0 }"#,
        r#"{ "max_domains_per_address": 1 }"#,
        r#"{ "max_ttl_seconds": 0 }"#,
    ] {
        let raw = format!(
            r#"{{
                "runtime": {{
                    "dns": {{
                        "servers": {{ "primary": {{ "type": "udp", "host": "1.1.1.1" }} }},
                        "default_server": "primary",
                        "reverse_mapping": {reverse_mapping}
                    }}
                }},
                "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
            }}"#
        );
        let error = RuntimeConfig::parse(&raw).expect_err("invalid reverse mapping must fail");
        assert!(matches!(error, zero_config::ConfigError::InvalidDns(_)));
    }
}

#[test]
fn rejects_fake_ip_pool_overlapping_tun_owned_addresses() {
    for addr in ["198.18.0.1/24", "198.18.0.2/24"] {
        let raw = format!(
            r#"{{
                "runtime": {{
                    "dns": {{
                        "servers": {{ "global": {{ "type": "udp", "host": "1.1.1.1" }} }},
                        "default_server": "global",
                        "answer": {{ "type": "fake_ip" }}
                    }},
                    "tun": {{ "addr": "{addr}", "dual_stack": false }}
                }},
                "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
            }}"#
        );
        let error = RuntimeConfig::parse(&raw).expect_err("overlap must fail early");
        assert!(matches!(error, zero_config::ConfigError::InvalidRuntime(_)));
        assert!(error.to_string().contains("overlaps TUN-owned address"));
    }
}

#[test]
fn validates_ipv6_fake_ip_pool_and_tun_overlap() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": { "global": { "type": "udp", "host": "1.1.1.1" } },
                    "default_server": "global",
                    "answer": {
                        "type": "fake_ip",
                        "cidr": "198.18.0.0/15",
                        "ipv6_cidr": "fd00::/120",
                        "max_entries": 128
                    }
                },
                "tun": { "addr": "10.66.0.1/24", "secondary_addr": "fd66::1/64" }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("dual-stack Fake-IP pools should parse");
    assert_eq!(
        config
            .runtime
            .dns
            .as_ref()
            .and_then(|dns| dns.fake_ip())
            .and_then(|fake_ip| fake_ip.ipv6_cidr),
        Some("fd00::/120")
    );

    for ipv6_cidr in ["198.19.0.0/24", "fd00::/127", "fd66::/64"] {
        let raw = format!(
            r#"{{
                "runtime": {{
                    "dns": {{
                        "servers": {{ "global": {{ "type": "udp", "host": "1.1.1.1" }} }},
                        "default_server": "global",
                        "answer": {{
                            "type": "fake_ip",
                            "ipv6_cidr": "{ipv6_cidr}"
                        }}
                    }},
                    "tun": {{ "addr": "10.66.0.1/24", "secondary_addr": "fd66::1/64" }}
                }},
                "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
            }}"#
        );
        RuntimeConfig::parse(&raw).expect_err("invalid or overlapping IPv6 Fake-IP pool must fail");
    }
}

#[test]
fn parses_named_dns_dispatch_with_shared_conditions() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": {
                        "cn": { "type": "udp", "host": "223.5.5.5" },
                        "global": { "type": "udp", "host": "1.1.1.1" }
                    },
                    "default_server": "global",
                    "dispatch": [{
                        "condition": {
                            "type": "or",
                            "items": [
                                { "type": "domain", "values": ["cn"] },
                                { "type": "domain_keyword", "values": ["intranet"] }
                            ]
                        },
                        "server": "cn"
                    }]
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("named DNS dispatch should parse");

    let dispatch = config
        .compile_dns_dispatch()
        .expect("compile DNS dispatch")
        .expect("configured DNS");
    assert_eq!(dispatch.select("open.bigmodel.cn"), "cn");
    assert_eq!(dispatch.select("api.intranet.example"), "cn");
    assert_eq!(dispatch.select("example.com"), "global");
}

#[test]
fn rejects_dns_dispatch_conditions_without_pre_resolution_facts() {
    let error = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": { "global": { "type": "udp", "host": "1.1.1.1" } },
                    "default_server": "global",
                    "dispatch": [{
                        "condition": { "type": "ip", "values": ["1.1.1.0/24"] },
                        "server": "global"
                    }]
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect_err("DNS IP dispatch must fail before runtime");
    assert!(matches!(error, zero_config::ConfigError::InvalidDns(_)));
    assert!(error
        .to_string()
        .contains("unavailable before DNS resolution"));
}

#[test]
fn versioned_tun_client_example_is_valid() {
    RuntimeConfig::parse(include_str!("../../../examples/v0.1.0/tun-client.json"))
        .expect("versioned declarative TUN example must remain valid");
}

#[test]
fn rejects_undefined_outbound_reference() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "missing" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::UndefinedRouteTargetTag { .. }
    ));
}

#[test]
fn rejects_removed_protocol_and_action_aliases() {
    let protocol_error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "http-in",
                    "listen": { "address": "127.0.0.1", "port": 8080 },
                    "protocol": { "type": "http_connect" }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("legacy http_connect protocol name should be rejected");

    assert!(matches!(
        protocol_error,
        zero_config::ConfigError::ParseConfig(_)
    ));

    let action_error = RuntimeConfig::parse(
        r#"{
            "route": {
                "rules": [],
                "final": { "type": "block" }
            }
        }"#,
    )
    .expect_err("block action alias should be rejected");

    assert!(matches!(
        action_error,
        zero_config::ConfigError::ParseConfig(_)
    ));
}

#[test]
fn accepts_mixed_inbound_type() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "mixed-in",
                    "listen": { "address": "127.0.0.1", "port": 1080 },
                    "protocol": { "type": "mixed" }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    assert!(matches!(
        config.inbounds[0].protocol,
        InboundProtocolConfig::Mixed { .. }
    ));
}

#[test]
fn parses_socks5_inbound_and_outbound_auth() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "socks-in",
                    "listen": { "address": "127.0.0.1", "port": 1080 },
                    "protocol": {
                        "type": "socks5",
                        "users": [
                            { "username": "alice", "password": "secret" }
                        ]
                    }
                },
                {
                    "tag": "mixed-in",
                    "listen": { "address": "127.0.0.1", "port": 1081 },
                    "protocol": {
                        "type": "mixed",
                        "socks5_users": [
                            { "password": "mixed-secret" }
                        ]
                    }
                }
            ],
            "outbounds": [
                {
                    "tag": "chain",
                    "protocol": {
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": 2080,
                        "password": "upstream-secret"
                    }
                },
                {
                    "tag": "no-auth-chain",
                    "protocol": {
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": 2081
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "chain" }
            }
        }"#,
    )
    .expect("config should parse");

    assert_eq!(
        config.inbounds[0].protocol.socks5_users()[0].username,
        "alice"
    );
    assert_eq!(
        config.inbounds[1].protocol.socks5_users()[0].username,
        "mixed-secret"
    );
    assert_eq!(
        config.inbounds[1].protocol.socks5_users()[0].password,
        "mixed-secret"
    );
    match &config.outbounds[0].protocol {
        OutboundProtocolConfig::Socks5 {
            username, password, ..
        } => {
            assert_eq!(username.as_deref(), Some("upstream-secret"));
            assert_eq!(password.as_deref(), Some("upstream-secret"));
        }
        _ => panic!("expected socks5 outbound"),
    }
    match &config.outbounds[1].protocol {
        OutboundProtocolConfig::Socks5 {
            username, password, ..
        } => {
            assert_eq!(username, &None);
            assert_eq!(password, &None);
        }
        _ => panic!("expected socks5 outbound"),
    }
}

#[test]
fn parses_mieru_username_defaults_from_password() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "mieru-in",
                    "listen": { "address": "127.0.0.1", "port": 2998 },
                    "protocol": {
                        "type": "mieru",
                        "users": [
                            {
                                "password": "inbound-secret",
                                "principal_key": "subscription:42"
                            }
                        ]
                    }
                }
            ],
            "outbounds": [
                {
                    "tag": "mieru-node",
                    "protocol": {
                        "type": "mieru",
                        "server": "example.com",
                        "port": 2999,
                        "password": "318149df-2bab-4a35-9de1-870f3e410598"
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "mieru-node" }
            }
        }"#,
    )
    .expect("config should parse");

    match &config.outbounds[0].protocol {
        OutboundProtocolConfig::Mieru {
            username, password, ..
        } => {
            assert_eq!(
                username.as_deref(),
                Some("318149df-2bab-4a35-9de1-870f3e410598")
            );
            assert_eq!(password, "318149df-2bab-4a35-9de1-870f3e410598");
        }
        _ => panic!("expected mieru outbound"),
    }
    match &config.inbounds[0].protocol {
        InboundProtocolConfig::Mieru { users } => {
            assert_eq!(users[0].username, "inbound-secret");
            assert_eq!(users[0].password, "inbound-secret");
            assert_eq!(users[0].principal_key.as_deref(), Some("subscription:42"));
        }
        _ => panic!("expected mieru inbound"),
    }
}

#[test]
fn rejects_duplicate_mieru_effective_principal_keys() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "mieru-in",
                    "listen": { "address": "127.0.0.1", "port": 2998 },
                    "protocol": {
                        "type": "mieru",
                        "users": [
                            {
                                "username": "first",
                                "password": "first-secret",
                                "principal_key": "subscription:42"
                            },
                            {
                                "username": "second",
                                "password": "second-secret",
                                "principal_key": "subscription:42"
                            }
                        ]
                    }
                }
            ],
            "outbounds": [
                {
                    "tag": "direct",
                    "protocol": { "type": "direct" }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "direct" }
            }
        }"#,
    )
    .expect_err("duplicate effective Mieru principals should be rejected");

    assert!(
        error
            .to_string()
            .contains("duplicate effective principal_key `subscription:42`"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_partial_socks5_outbound_auth() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "chain",
                    "protocol": {
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": 2080,
                        "username": "upstream"
                    }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "route", "outbound": "chain" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::InvalidOutbound(_)
    ));
}

#[test]
fn rejects_duplicate_inbound_listen_endpoint() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "socks-in",
                    "listen": { "address": "127.0.0.1", "port": 1080 },
                    "protocol": { "type": "socks5" }
                },
                {
                    "tag": "http-in",
                    "listen": { "address": "127.0.0.1", "port": 1080 },
                    "protocol": { "type": "http" }
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::DuplicateInboundListen { .. }
    ));
}

#[test]
fn accepts_vless_vision_outbound_with_reality() {
    RuntimeConfig::parse(
        r#"{
            "outbounds": [{
                "tag": "vision",
                "protocol": {
                    "type": "vless",
                    "server": "127.0.0.1",
                    "port": 443,
                    "id": "11111111-2222-3333-4444-555555555555",
                    "flow": "xtls-rprx-vision",
                    "reality": {
                        "public_key": "9AwHi13y1rN6EWTSo8-HNCOhrzr251jNY7SSIxo0diA",
                        "short_id": "0123456789abcdef",
                        "server_name": "example.com"
                    }
                }
            }],
            "route": { "final": { "type": "route", "outbound": "vision" } }
        }"#,
    )
    .expect("REALITY-backed Vision outbound should be valid");
}

#[test]
fn rejects_vless_vision_without_reality() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [{
                "tag": "vision",
                "protocol": {
                    "type": "vless",
                    "server": "127.0.0.1",
                    "port": 443,
                    "id": "11111111-2222-3333-4444-555555555555",
                    "flow": "xtls-rprx-vision"
                }
            }],
            "route": { "final": { "type": "route", "outbound": "vision" } }
        }"#,
    )
    .expect_err("Vision without REALITY should fail early");
    assert!(error.to_string().contains("requires `reality`"));
}

#[test]
fn rejects_vless_vision_with_mux() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [{
                "tag": "vision",
                "protocol": {
                    "type": "vless",
                    "server": "127.0.0.1",
                    "port": 443,
                    "id": "11111111-2222-3333-4444-555555555555",
                    "flow": "xtls-rprx-vision",
                    "mux_concurrency": 8,
                    "reality": {
                        "public_key": "9AwHi13y1rN6EWTSo8-HNCOhrzr251jNY7SSIxo0diA",
                        "short_id": "0123456789abcdef",
                        "server_name": "example.com"
                    }
                }
            }],
            "route": { "final": { "type": "route", "outbound": "vision" } }
        }"#,
    )
    .expect_err("Vision with MUX should fail early");
    assert!(error.to_string().contains("cannot be combined"));
}

#[test]
fn rejects_obsolete_vless_vision_udp443_name() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [{
                "tag": "vision",
                "protocol": {
                    "type": "vless",
                    "server": "127.0.0.1",
                    "port": 443,
                    "id": "11111111-2222-3333-4444-555555555555",
                    "flow": "xtls-rprx-vision-udp443"
                }
            }],
            "route": { "final": { "type": "route", "outbound": "vision" } }
        }"#,
    )
    .expect_err("obsolete flow name should fail early");
    assert!(error.to_string().contains("obsolete"));
}

#[test]
fn accepts_explicit_zero_aead_v1_name() {
    RuntimeConfig::parse(
        r#"{
            "outbounds": [{
                "tag": "zero-private",
                "protocol": {
                    "type": "vless",
                    "server": "127.0.0.1",
                    "port": 443,
                    "id": "11111111-2222-3333-4444-555555555555",
                    "flow": "zero-aead-v1"
                }
            }],
            "route": { "final": { "type": "route", "outbound": "zero-private" } }
        }"#,
    )
    .expect("explicit Zero-private flow should remain available");
}

#[test]
fn rejects_vless_vision_inbound_without_reality() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [{
                "tag": "vision-in",
                "listen": { "address": "127.0.0.1", "port": 2443 },
                "protocol": {
                    "type": "vless",
                    "users": [{
                        "id": "11111111-2222-3333-4444-555555555555",
                        "flow": "xtls-rprx-vision"
                    }]
                }
            }],
            "route": { "final": { "type": "direct" } }
        }"#,
    )
    .expect_err("Vision inbound without REALITY should fail early");
    assert!(error
        .to_string()
        .contains("inbound flow `xtls-rprx-vision` requires `reality`"));
}

#[test]
fn parses_native_trojan_mux_policy() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [{
                "tag": "trojan-in",
                "listen": { "address": "127.0.0.1", "port": 443 },
                "protocol": {
                    "type": "trojan",
                    "password": "secret",
                    "mux_response_backlog_frames": 64,
                    "mux_response_backlog_bytes": 1048576,
                    "tls": { "cert_path": "cert.pem", "key_path": "key.pem" }
                }
            }],
            "outbounds": [{
                "tag": "trojan-mux",
                "protocol": {
                    "type": "trojan",
                    "server": "127.0.0.1",
                    "port": 443,
                    "password": "secret",
                    "mux_concurrency": 8,
                    "mux_idle_timeout_secs": 30,
                    "mux_response_backlog_frames": 64,
                    "mux_response_backlog_bytes": 1048576
                }
            }],
            "route": { "final": { "type": "route", "outbound": "trojan-mux" } }
        }"#,
    )
    .expect("native Trojan MUX policy should parse");

    match &config.outbounds[0].protocol {
        OutboundProtocolConfig::Trojan {
            mux_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            ..
        } => {
            assert_eq!(*mux_concurrency, Some(8));
            assert_eq!(*mux_idle_timeout_secs, Some(30));
            assert_eq!(*mux_response_backlog_frames, Some(64));
            assert_eq!(*mux_response_backlog_bytes, Some(1024 * 1024));
        }
        _ => panic!("expected trojan outbound"),
    }
}

#[test]
fn rejects_invalid_native_trojan_mux_policy() {
    for fragment in [
        r#""mux_concurrency": 0"#,
        r#""mux_idle_timeout_secs": 0"#,
        r#""mux_response_backlog_frames": 0"#,
        r#""mux_response_backlog_bytes": 16383"#,
    ] {
        let config = format!(
            r#"{{
                "outbounds": [{{
                    "tag": "trojan-mux",
                    "protocol": {{
                        "type": "trojan",
                        "server": "127.0.0.1",
                        "port": 443,
                        "password": "secret",
                        {fragment}
                    }}
                }}],
                "route": {{ "final": {{ "type": "route", "outbound": "trojan-mux" }} }}
            }}"#
        );
        assert!(
            RuntimeConfig::parse(&config).is_err(),
            "{fragment} should fail"
        );
    }
}

#[test]
fn accepts_vless_vision_inbound_with_reality() {
    RuntimeConfig::parse(
        r#"{
            "inbounds": [{
                "tag": "vision-in",
                "listen": { "address": "127.0.0.1", "port": 2443 },
                "protocol": {
                    "type": "vless",
                    "users": [{
                        "id": "11111111-2222-3333-4444-555555555555",
                        "flow": "xtls-rprx-vision"
                    }],
                    "reality": {
                        "private_key": "OKMOFBeltHBXaTQ8cIcsgabVQcqXeTB9Ih3lPtWMY04",
                        "short_ids": ["0123456789abcdef"],
                        "server_name": "www.cloudflare.com"
                    }
                }
            }],
            "route": { "final": { "type": "direct" } }
        }"#,
    )
    .expect("Vision inbound over REALITY should parse");
}

#[test]
fn parses_utf8_bom_prefixed_json() {
    let config = RuntimeConfig::parse(
        "\u{feff}{\n  \"inbounds\": [],\n  \"route\": { \"rules\": [], \"final\": { \"type\": \"direct\" } }\n}",
    )
    .expect("config with utf-8 bom should parse");

    assert!(config.inbounds.is_empty());
    assert!(matches!(
        config.route.final_action,
        RouteActionConfig::Direct
    ));
}

#[test]
fn selector_group_requires_defined_member_outbounds() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "direct",
                    "protocol": { "type": "direct" }
                }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["missing"]
                }
            ],
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::InvalidOutboundGroup(_)
    ));
}

#[test]
fn global_mode_accepts_selector_group_target() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "direct",
                    "protocol": { "type": "direct" }
                }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["direct"]
                }
            ],
            "mode": {
                "type": "global",
                "outbound": "proxy"
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    assert!(matches!(config.mode, ModeConfig::Global { .. }));
}

#[test]
fn accepts_fallback_group_type() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "direct",
                    "protocol": { "type": "direct" }
                },
                {
                    "tag": "chain",
                    "protocol": { "type": "socks5", "server": "127.0.0.1", "port": 2080 }
                }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "fallback",
                    "outbounds": ["chain", "direct"]
                }
            ],
            "mode": {
                "type": "global",
                "outbound": "proxy"
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    assert!(matches!(
        config.outbound_groups[0].group,
        OutboundGroupKind::Fallback { .. }
    ));
}

#[test]
fn accepts_urltest_group_type() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "direct",
                    "protocol": { "type": "direct" }
                },
                {
                    "tag": "chain",
                    "protocol": { "type": "socks5", "server": "127.0.0.1", "port": 2080 }
                }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "url_test",
                    "outbounds": ["chain", "direct"],
                    "url": "http://127.0.0.1:8081/",
                    "interval_seconds": 15,
                    "tolerance_ms": 20
                }
            ],
            "mode": {
                "type": "global",
                "outbound": "proxy"
            },
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    let OutboundGroupKind::UrlTest { tolerance_ms, .. } = &config.outbound_groups[0].group else {
        panic!("expected url_test group");
    };
    assert_eq!(*tolerance_ms, 20);
}

#[test]
fn accepts_urltest_without_own_url() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "url_test",
                    "outbounds": ["direct"]
                }
            ],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("config should parse");

    let OutboundGroupKind::UrlTest {
        url, tolerance_ms, ..
    } = &config.outbound_groups[0].group
    else {
        panic!("expected url_test group");
    };
    assert!(url.is_none());
    assert_eq!(*tolerance_ms, 0);
}

#[test]
fn accepts_loadbalance_group_type() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } },
                { "tag": "chain", "protocol": { "type": "socks5", "server": "127.0.0.1", "port": 2080 } }
            ],
            "outbound_groups": [
                {
                    "tag": "proxy",
                    "type": "load_balance",
                    "outbounds": ["chain", "direct"],
                    "strategy": "round_robin"
                }
            ],
            "mode": { "type": "global", "outbound": "proxy" },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("config should parse");

    let OutboundGroupKind::LoadBalance {
        outbounds,
        default,
        strategy,
    } = &config.outbound_groups[0].group
    else {
        panic!("expected loadbalance group");
    };
    assert_eq!(outbounds.len(), 2);
    assert!(default.is_none());
    assert!(matches!(strategy, LoadBalanceStrategy::RoundRobin));
}

#[test]
fn loadbalance_group_defaults_to_round_robin_strategy() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } },
                { "tag": "s5", "protocol": { "type": "socks5", "server": "127.0.0.1", "port": 2080 } }
            ],
            "outbound_groups": [
                {
                    "tag": "lb",
                    "type": "load_balance",
                    "outbounds": ["s5", "direct"]
                }
            ],
            "mode": { "type": "global", "outbound": "lb" },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("config should parse");

    let OutboundGroupKind::LoadBalance { strategy, .. } = &config.outbound_groups[0].group else {
        panic!("expected loadbalance group");
    };
    assert!(matches!(strategy, LoadBalanceStrategy::RoundRobin));
}

#[test]
fn accepts_loadbalance_random_strategy() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } },
                { "tag": "s5", "protocol": { "type": "socks5", "server": "127.0.0.1", "port": 2080 } }
            ],
            "outbound_groups": [
                {
                    "tag": "lb",
                    "type": "load_balance",
                    "outbounds": ["s5", "direct"],
                    "strategy": "random"
                }
            ],
            "mode": { "type": "global", "outbound": "lb" },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("config should parse");

    let OutboundGroupKind::LoadBalance { strategy, .. } = &config.outbound_groups[0].group else {
        panic!("expected loadbalance group");
    };
    assert!(matches!(strategy, LoadBalanceStrategy::Random));
}

#[test]
fn loadbalance_group_with_default() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } },
                { "tag": "s5", "protocol": { "type": "socks5", "server": "127.0.0.1", "port": 2080 } }
            ],
            "outbound_groups": [
                {
                    "tag": "lb",
                    "type": "load_balance",
                    "outbounds": ["s5", "direct"],
                    "default": "direct"
                }
            ],
            "mode": { "type": "global", "outbound": "lb" },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("config should parse");

    assert_eq!(config.outbound_groups[0].active_outbound(), Some("direct"));
}

#[test]
fn loadbalance_group_requires_defined_member_outbounds() {
    let result = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "direct", "protocol": { "type": "direct" } }
            ],
            "outbound_groups": [
                {
                    "tag": "lb",
                    "type": "load_balance",
                    "outbounds": ["missing", "direct"]
                }
            ],
            "mode": { "type": "global", "outbound": "lb" },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    );
    assert!(result.is_err());
}

#[test]
fn accepts_group_member_referencing_another_group() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "direct",
                    "protocol": { "type": "direct" }
                },
                {
                    "tag": "block",
                    "protocol": { "type": "block" }
                }
            ],
            "outbound_groups": [
                {
                    "tag": "fallback-proxy",
                    "type": "fallback",
                    "outbounds": ["block", "direct"]
                },
                {
                    "tag": "proxy",
                    "type": "selector",
                    "outbounds": ["fallback-proxy", "direct"],
                    "selected": "fallback-proxy"
                }
            ],
            "mode": {
                "type": "global",
                "outbound": "proxy"
            },
            "route": {
                "rules": [],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect("config should parse");

    assert_eq!(config.outbound_groups.len(), 2);
    assert!(matches!(
        config.outbound_groups[0].group,
        OutboundGroupKind::Fallback { .. }
    ));
    assert!(matches!(
        config.outbound_groups[1].group,
        OutboundGroupKind::Selector { .. }
    ));
}

#[test]
fn rejects_group_reference_cycle() {
    let error = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {
                    "tag": "direct",
                    "protocol": { "type": "direct" }
                }
            ],
            "outbound_groups": [
                {
                    "tag": "group-a",
                    "type": "selector",
                    "outbounds": ["group-b"],
                    "selected": "group-b"
                },
                {
                    "tag": "group-b",
                    "type": "fallback",
                    "outbounds": ["group-a"]
                }
            ],
            "mode": {
                "type": "global",
                "outbound": "group-a"
            },
            "route": {
                "rules": [],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::InvalidOutboundGroup(_)
    ));
}

#[test]
fn loads_rule_set_from_relative_file_path() {
    let project_dir = temp_test_dir("config-rule-set-relative");
    let rules_dir = project_dir.join("rules");
    fs::create_dir_all(&rules_dir).expect("create rules dir");
    fs::write(rules_dir.join("ads.txt"), "blocked.example\n.ads.local\n").expect("write rules");

    let config_path = project_dir.join("config.json");
    fs::write(
        &config_path,
        r#"{
            "outbounds": [
                { "tag": "block", "protocol": { "type": "block" } }
            ],
            "route": {
                "rule_sets": [
                    {
                        "tag": "ads",
                        "type": "file",
                        "path": "rules/ads.txt",
                        "format": "domain_list"
                    }
                ],
                "rules": [
                    {
                        "condition": { "type": "rule_set", "tag": "ads" },
                        "action": { "type": "route", "outbound": "block" }
                    }
                ],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("write config");

    let config = RuntimeConfig::load_from_path(&config_path).expect("load config");

    assert_eq!(config.source_dir(), Some(project_dir.as_path()));
    assert!(matches!(
        config.route.rules[0].condition,
        RuleConditionConfig::RuleSet { .. }
    ));

    cleanup_temp_dir(&project_dir);
}

#[test]
fn accepts_historical_empty_route_rule_sets_from_old_clients() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {
                "rule_sets": [],
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("historical route.rule_sets should remain accepted");

    assert!(config.route.rule_sets.is_empty());
}

#[test]
fn shares_one_domain_rule_set_between_dns_dispatch_and_traffic_route() {
    let project_dir = temp_test_dir("config-shared-dns-rule-set");
    let rules_dir = project_dir.join("rules");
    fs::create_dir_all(&rules_dir).expect("create rules dir");
    fs::write(rules_dir.join("cn.txt"), "cn\nbigmodel.cn\n").expect("write rules");

    let config_path = project_dir.join("config.json");
    fs::write(
        &config_path,
        r#"{
            "runtime": {
                "dns": {
                    "servers": {
                        "cn": { "type": "udp", "host": "223.5.5.5" },
                        "global": { "type": "udp", "host": "1.1.1.1" }
                    },
                    "default_server": "global",
                    "dispatch": [{
                        "condition": { "type": "rule_set", "tag": "cn-domains" },
                        "server": "cn"
                    }]
                }
            },
            "route": {
                "rule_sets": [{
                    "tag": "cn-domains",
                    "type": "file",
                    "path": "rules/cn.txt",
                    "format": "domain_list"
                }],
                "rules": [{
                    "condition": { "type": "rule_set", "tag": "cn-domains" },
                    "action": { "type": "direct" }
                }],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect("write config");

    let config = RuntimeConfig::load_from_path(&config_path).expect("load shared rule set");
    let dispatch = config
        .compile_dns_dispatch()
        .expect("compile DNS dispatch")
        .expect("configured DNS");
    assert_eq!(dispatch.select("open.bigmodel.cn"), "cn");
    assert_eq!(
        config
            .compile_route()
            .expect("compile traffic route")
            .rules
            .len(),
        1
    );

    cleanup_temp_dir(&project_dir);
}

#[test]
fn rejects_undefined_rule_set_reference() {
    let error = RuntimeConfig::parse(
        r#"{
            "route": {
                "rules": [
                    {
                        "condition": { "type": "rule_set", "tag": "ads" },
                        "action": { "type": "direct" }
                    }
                ],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::UndefinedRuleSetTag { .. }
    ));
}

#[test]
fn parses_inbound_route_condition() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "hk-in",
                    "listen": { "address": "127.0.0.1", "port": 7891 },
                    "protocol": { "type": "mixed" }
                }
            ],
            "outbounds": [
                { "tag": "hk-out", "protocol": { "type": "direct" } }
            ],
            "route": {
                "rules": [
                    {
                        "condition": { "type": "inbound", "values": ["hk-in"] },
                        "action": { "type": "route", "outbound": "hk-out" }
                    }
                ],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("config should parse");

    assert!(matches!(
        config.route.rules[0].condition,
        RuleConditionConfig::Inbound { .. }
    ));
}

#[test]
fn rejects_undefined_inbound_route_condition_reference() {
    let error = RuntimeConfig::parse(
        r#"{
            "inbounds": [
                {
                    "tag": "hk-in",
                    "listen": { "address": "127.0.0.1", "port": 7891 },
                    "protocol": { "type": "mixed" }
                }
            ],
            "route": {
                "rules": [
                    {
                        "condition": { "type": "inbound", "values": ["missing-in"] },
                        "action": { "type": "direct" }
                    }
                ],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect_err("config should fail");

    assert!(matches!(
        error,
        zero_config::ConfigError::InvalidRuleCondition(_)
    ));
    assert!(error.to_string().contains("missing-in"));
}

#[test]
fn rejects_invalid_cidr_rule_set_entry() {
    let project_dir = temp_test_dir("config-rule-set-invalid-cidr");
    let rules_dir = project_dir.join("rules");
    fs::create_dir_all(&rules_dir).expect("create rules dir");
    fs::write(rules_dir.join("lan.txt"), "10.0.0.0/8\nnot-a-cidr\n").expect("write rules");

    let config_path = project_dir.join("config.json");
    fs::write(
        &config_path,
        r#"{
            "route": {
                "rule_sets": [
                    {
                        "tag": "lan",
                        "type": "file",
                        "path": "rules/lan.txt",
                        "format": "cidr_list"
                    }
                ],
                "rules": [
                    {
                        "condition": { "type": "rule_set", "tag": "lan" },
                        "action": { "type": "direct" }
                    }
                ],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("write config");

    let error = RuntimeConfig::load_from_path(&config_path).expect_err("config should fail");
    assert!(matches!(error, zero_config::ConfigError::InvalidRuleSet(_)));

    cleanup_temp_dir(&project_dir);
}

#[test]
fn rejects_zrs_with_invalid_full_checksum() {
    let project_dir = temp_test_dir("config-rule-set-invalid-zrs-checksum");
    let matcher_path = project_dir.join("corrupt.zrs");
    let (compiled, _) = zero_rule::RuleSetCompiler
        .compile(zero_rule::RuleSet::new(vec![zero_rule::Rule::DomainExact(
            "blocked.example".to_owned(),
        )]))
        .expect("compile matcher");
    let mut artifact = zero_rule::zrs::encode(&compiled).expect("encode ZRS");
    let last = artifact.last_mut().expect("non-empty ZRS");
    *last ^= 0xff;
    fs::write(&matcher_path, artifact).expect("write corrupt ZRS");

    let error = RuntimeConfig::parse(&format!(
        r#"{{
            "route": {{
                "rule_sets": [{{
                    "tag": "corrupt",
                    "type": "file",
                    "path": "{}",
                    "format": "zrs"
                }}],
                "rules": [{{
                    "condition": {{ "type": "rule_set", "tag": "corrupt" }},
                    "action": {{ "type": "direct" }}
                }}],
                "final": {{ "type": "direct" }}
            }}
        }}"#,
        escape_json_path(&matcher_path),
    ))
    .expect_err("corrupt ZRS should fail");

    assert!(matches!(error, zero_config::ConfigError::InvalidRuleSet(_)));
    assert!(error.to_string().contains("checksum"));

    cleanup_temp_dir(&project_dir);
}

fn temp_test_dir(prefix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{prefix}-{nonce}"));
    fs::create_dir_all(&dir).expect("create temp test dir");
    dir
}

fn escape_json_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "\\\\")
}

fn cleanup_temp_dir(path: &Path) {
    let _ = fs::remove_dir_all(path);
}
