use std::net::{IpAddr, Ipv4Addr};

use serde_json::{Value, json};
use zero_config::{OutboundAddressFamily, OutboundDialConfig, RuntimeConfig};
use zero_traits::AddressFamily;

fn config(protocol: Value, dial: Option<Value>) -> Result<RuntimeConfig, zero_config::ConfigError> {
    let mut outbound = json!({"tag": "egress", "protocol": protocol});
    if let Some(dial) = dial {
        outbound["dial"] = dial;
    }
    RuntimeConfig::parse(
        &json!({
            "outbounds": [outbound],
            "route": {"rules": [], "final": {"type": "route", "outbound": "egress"}}
        })
        .to_string(),
    )
}

fn direct(dial: Value) -> Result<RuntimeConfig, zero_config::ConfigError> {
    config(json!({"type": "direct"}), Some(dial))
}

#[test]
fn omitted_empty_and_explicit_auto_preserve_legacy_dial_behavior() {
    let omitted = config(json!({"type": "direct"}), None).unwrap();
    for dial in [json!({}), json!({"address_family": "auto"})] {
        let explicit = direct(dial).unwrap();
        assert_eq!(explicit, omitted);
        assert!(explicit.outbounds[0].dial.is_default());
        let serialized = serde_json::to_value(&explicit).unwrap();
        assert!(serialized["outbounds"][0].get("dial").is_none());
    }
}

#[test]
fn dial_constraints_are_a_common_field_with_direct_only_nondefault_support() {
    for protocol in [
        json!({"type": "block"}),
        json!({"type": "socks5", "server": "127.0.0.1", "port": 1080}),
    ] {
        config(protocol.clone(), None).unwrap();
        config(protocol.clone(), Some(json!({"address_family": "auto"}))).unwrap();
        for dial in [
            json!({"address_family": "only_ipv4"}),
            json!({"address_family": "only_ipv6"}),
            json!({"interface": "Ethernet 2"}),
            json!({"source_ip": "127.0.0.1"}),
        ] {
            let error = config(protocol.clone(), Some(dial))
                .unwrap_err()
                .to_string();
            assert!(error.contains("require a direct outbound"), "{error}");
            assert!(error.contains("outbounds[0] `egress`"), "{error}");
        }
    }
}

#[test]
fn dial_parsing_rejects_unknown_keys_families_and_non_ip_sources() {
    for dial in [
        json!({"unknown": true}),
        json!({"address_family": "prefer_ipv4"}),
        json!({"address_family": "ipv6"}),
        json!({"source_ip": "localhost"}),
        json!({"source_ip": "192.0.2.1/24"}),
        json!({"source_ip": "[::1]"}),
        json!({"source_ip": "fe80::1%eth0"}),
        json!({"source_ip": "127.0.0.1:8080"}),
    ] {
        assert!(direct(dial.clone()).is_err(), "accepted {dial}");
    }
}

#[test]
fn dial_validation_rejects_invalid_interface_and_nonunicast_source() {
    for interface in ["", " ", " eth0", "eth0 ", "eth\u{0}0", "eth/0"] {
        assert!(
            direct(json!({"interface": interface})).is_err(),
            "{interface:?}"
        );
    }
    for source in [
        "0.0.0.0",
        "::",
        "224.0.0.1",
        "ff02::1",
        "255.255.255.255",
        "::ffff:0.0.0.0",
        "::ffff:224.0.0.1",
        "::ffff:255.255.255.255",
    ] {
        assert!(direct(json!({"source_ip": source})).is_err(), "{source}");
    }
}

#[test]
fn source_family_is_canonicalized_and_must_satisfy_explicit_family() {
    let parsed = direct(json!({
        "address_family": "only_ipv4", "source_ip": "::ffff:192.0.2.10"
    }))
    .unwrap();
    let dial = &parsed.outbounds[0].dial;
    assert_eq!(dial.address_family, OutboundAddressFamily::OnlyIpv4);
    assert_eq!(
        dial.source_ip,
        Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)))
    );
    assert_eq!(
        serde_json::to_value(dial).unwrap()["source_ip"],
        "192.0.2.10"
    );
    for (family, source) in [
        ("only_ipv4", "::1"),
        ("only_ipv6", "127.0.0.1"),
        ("only_ipv6", "::ffff:192.0.2.10"),
    ] {
        let error = direct(json!({"address_family": family, "source_ip": source}))
            .unwrap_err()
            .to_string();
        assert!(error.contains("conflicts with address_family"), "{error}");
    }
    for (source, family) in [
        ("127.0.0.1", AddressFamily::OnlyIpv4),
        ("::ffff:127.0.0.1", AddressFamily::OnlyIpv4),
        ("::1", AddressFamily::OnlyIpv6),
    ] {
        let parsed = direct(json!({"source_ip": source})).unwrap();
        assert_eq!(
            parsed.outbounds[0].dial.to_policy().effective_family(),
            Ok(family)
        );
    }
}

#[test]
fn validation_is_structural_and_does_not_inspect_local_interfaces() {
    let parsed = direct(json!({
        "address_family": "only_ipv6", "interface": "Uninstalled Adapter 2",
        "source_ip": "2001:db8::10"
    }))
    .expect("local ownership is a platform execution concern");
    assert_eq!(
        parsed.outbounds[0].dial.interface.as_deref(),
        Some("Uninstalled Adapter 2")
    );
    let roundtrip = RuntimeConfig::parse(&serde_json::to_string(&parsed).unwrap()).unwrap();
    assert_eq!(roundtrip, parsed);
}

#[test]
fn programmatically_constructed_mapped_sources_project_to_ipv4() {
    let dial = OutboundDialConfig {
        source_ip: Some("::ffff:127.0.0.1".parse().unwrap()),
        ..OutboundDialConfig::default()
    };
    assert_eq!(
        dial.to_policy().source_ip,
        Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
    );
}

#[test]
fn versioned_direct_dial_example_passes_structural_validation() {
    RuntimeConfig::parse(include_str!("../../../examples/v0.0.3/direct-dial.json")).unwrap();
}
