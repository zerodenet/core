use serde_json::json;
use zero_config::RuntimeConfig;
#[test]
fn tls_policy_survives_native_configuration_roundtrip() {
    let raw = json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":8443},"protocol":{"type":"vless","users":[{"id":"test"}],"tls":{"cert_path":"one.pem","key_path":"one.key","options":{"reject_unknown_sni":true,"certificates":[{"cert_path":"two.pem","key_path":"two.key","ocsp_path":"two.ocsp","usage":"issue","build_chain":true}],"parameters":{"min_version":"1.3","curve_preferences":["X25519MLKEM768"],"enable_session_resumption":true}}}}}],"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"proxy.test","port":443,"id":"test","tls":{"options":{"disable_system_roots":true,"pinned_peer_cert_sha256":["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="],"verify_peer_names":["certificate.test"],"parameters":{"max_version":"1.3","cipher_suites":["TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"]}}}}}],"route":{"final":{"type":"route","outbound":"node"}}});
    let config = RuntimeConfig::parse(&raw.to_string()).unwrap();
    let encoded = serde_json::to_value(config).unwrap();
    assert_eq!(
        encoded["outbounds"][0]["protocol"]["tls"]["options"]["verify_peer_names"],
        json!(["certificate.test"])
    );
    let certificate = &encoded["inbounds"][0]["protocol"]["tls"]["options"]["certificates"][0];
    assert_eq!(certificate["usage"], "issue");
    assert_eq!(certificate["build_chain"], true);
    RuntimeConfig::parse(&encoded.to_string()).unwrap();
}
#[test]
fn invalid_tls_policy_and_quic_version_are_rejected_before_runtime() {
    for options in [
        json!({"parameters":{"min_version":"1.3","max_version":"1.2"}}),
        json!({"parameters":{"cipher_suites":["made-up"]}}),
        json!({"parameters":{"curve_preferences":["made-up"]}}),
        json!({"pinned_peer_cert_sha256":["AA=="]}),
        json!({"verify_peer_names":["*.test"]}),
    ] {
        let raw = json!({"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"proxy.test","port":443,"id":"test","tls":{"options":options}}}],"route":{"final":{"type":"route","outbound":"node"}}});
        assert!(RuntimeConfig::parse(&raw.to_string()).is_err());
    }
    let raw = json!({"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"proxy.test","port":443,"id":"test","quic":{"client_options":{"parameters":{"max_version":"1.2"}}}}}],"route":{"final":{"type":"route","outbound":"node"}}});
    assert!(RuntimeConfig::parse(&raw.to_string()).is_err());
}

#[test]
fn automatic_ocsp_configuration_preserves_per_certificate_intervals_and_rejects_overflow() {
    for (interval, accepted) in [(60u64, true), (u64::MAX, false)] {
        let mut raw = json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":8443},"protocol":{"type":"vless","users":[{"id":"test"}],"tls":{"cert_path":"one.pem","key_path":"one.key","options":{"ocsp_stapling_secs":interval,"certificates":[{"cert_path":"two.pem","key_path":"two.key","ocsp_stapling_secs":120}]}}}}]});
        raw["route"] = json!({"final":{"type":"direct"}});
        let parsed = RuntimeConfig::parse(&raw.to_string());
        assert_eq!(parsed.is_ok(), accepted, "{parsed:?}");
        if let Ok(parsed) = parsed {
            let encoded = serde_json::to_value(parsed).unwrap();
            let options = &encoded["inbounds"][0]["protocol"]["tls"]["options"];
            assert_eq!(options["ocsp_stapling_secs"], 60);
            assert_eq!(options["certificates"][0]["ocsp_stapling_secs"], 120);
        }
    }
    let options = zero_config::ServerTlsOptionsConfig {
        certificates: vec![zero_config::TlsCertificateFilesConfig {
            cert_path: "cert.pem".into(),
            key_path: "key.pem".into(),
            ocsp_path: None,
            ocsp_stapling_secs: u64::MAX,
            usage: zero_config::TlsCertificateUsageConfig::Encipherment,
            build_chain: false,
        }],
        ..Default::default()
    };
    assert!(ztls::settings::validate_server(&options.to_options(), false).is_err());
}

#[test]
fn authority_issue_can_be_the_only_server_certificate_source() {
    let authority_only = json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":8443},"protocol":{"type":"vless","users":[{"id":"test"}],"tls":{"options":{"certificates":[{"cert_path":"ca.pem","key_path":"ca.key","usage":"issue","build_chain":true}]}}}}],"route":{"final":{"type":"direct"}}});
    RuntimeConfig::parse(&authority_only.to_string()).unwrap();

    for (cert_path, key_path) in [("leaf.pem", ""), ("", "leaf.key"), ("", "")] {
        let raw = json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":8443},"protocol":{"type":"vless","users":[{"id":"test"}],"tls":{"cert_path":cert_path,"key_path":key_path}}}],"route":{"final":{"type":"direct"}}});
        assert!(RuntimeConfig::parse(&raw.to_string()).is_err());
    }
}

#[test]
fn openssl_ech_and_legacy_options_roundtrip_and_select_capabilities_early() {
    let ech_keys = "ACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAH/g0AAwAAIA==";
    let raw = json!({
        "inbounds": [{
            "tag": "vless",
            "listen": {"address": "127.0.0.1", "port": 8443},
            "protocol": {
                "type": "vless",
                "users": [{"id": "test"}],
                "tls": {
                    "cert_path": "server.pem",
                    "key_path": "server.key",
                    "options": {"backend": "openssl", "ech_server_keys": ech_keys}
                }
            }
        }],
        "outbounds": [{
            "tag": "legacy",
            "protocol": {
                "type": "vless",
                "server": "legacy.test",
                "port": 443,
                "id": "test",
                "tls": {"options": {"backend": "auto", "parameters": {"min_version": "1.0", "max_version": "1.1"}}}
            }
        }],
        "route": {"final": {"type": "route", "outbound": "legacy"}}
    });
    let config = RuntimeConfig::parse(&raw.to_string()).unwrap();
    let debug = format!("{config:?}");
    assert!(!debug.contains(ech_keys));
    assert!(debug.contains("[redacted]"));
    let encoded = serde_json::to_value(config).unwrap();
    assert_eq!(
        encoded["inbounds"][0]["protocol"]["tls"]["options"]["backend"],
        "open_ssl"
    );
    assert_eq!(
        encoded["inbounds"][0]["protocol"]["tls"]["options"]["ech_server_keys"],
        ech_keys
    );
    assert_eq!(
        encoded["outbounds"][0]["protocol"]["tls"]["options"]["parameters"]["min_version"],
        "1.0"
    );
}

#[test]
fn unsupported_tls_backend_combinations_fail_during_config_validation() {
    let cases = [
        json!({"backend":"rustls","parameters":{"min_version":"1.0"}}),
        json!({"backend":"rustls","ech_server_keys":"ACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAH/g0AAwAAIA=="}),
        json!({"backend":"openssl","ech_server_keys":"not-base64"}),
    ];
    for options in cases {
        let raw = json!({
            "inbounds": [{
                "tag": "vless",
                "listen": {"address": "127.0.0.1", "port": 8443},
                "protocol": {
                    "type": "vless",
                    "users": [{"id": "test"}],
                    "tls": {"cert_path": "server.pem", "key_path": "server.key", "options": options}
                }
            }],
            "route": {"final": {"type": "direct"}}
        });
        assert!(RuntimeConfig::parse(&raw.to_string()).is_err());
    }

    let fingerprinted_legacy = json!({
        "outbounds": [{
            "tag": "legacy",
            "protocol": {
                "type": "vless",
                "server": "legacy.test",
                "port": 443,
                "id": "test",
                "tls": {
                    "client_fingerprint": "chrome",
                    "options": {"parameters": {"min_version": "1.0"}}
                }
            }
        }],
        "route": {"final": {"type": "route", "outbound": "legacy"}}
    });
    assert!(RuntimeConfig::parse(&fingerprinted_legacy.to_string()).is_err());
}
