use crate::inbound::tun::config::prepare_dns_routes;
use std::io;
use std::net::IpAddr;
use zero_config::RuntimeConfig;

fn config(mode: &str) -> RuntimeConfig {
    let mut value = serde_json::json!({"route":{"rules":[],"final":{"type":"direct"}}});
    if mode != "disabled" {
        value["runtime"] = serde_json::json!({"dns":{
            "servers":{"explicit":{"type":"udp","host":"1.1.1.1"}},
            "default_server":"explicit","answer":{"type":mode}
        }});
        if mode == "fake_ip" {
            value["runtime"]["dns"]["answer"]["cidr"] = "198.18.0.0/15".into();
        }
    }
    RuntimeConfig::parse(&value.to_string()).unwrap()
}

#[test]
fn dns_mode_interception_and_capture_matrix_keeps_system_egress_independent() {
    let system: Vec<IpAddr> = vec![
        "2408:8888::8".parse().unwrap(),
        "192.168.0.1".parse().unwrap(),
    ];
    for mode in ["disabled", "real", "fake_ip"] {
        for hijack in [false, true] {
            for capture in [false, true] {
                let result =
                    prepare_dns_routes(&config(mode), hijack, capture, || Ok(system.clone()));
                if mode == "disabled" && hijack {
                    assert!(result.is_err());
                    continue;
                }
                let (actual_hijack, endpoints) = result.unwrap();
                assert_eq!(actual_hijack, hijack);
                if !capture {
                    assert!(endpoints.is_empty());
                    continue;
                }
                if !hijack {
                    for address in &system {
                        assert!(endpoints.contains(address), "{mode}: {address}");
                    }
                }
                if mode != "disabled" {
                    assert!(endpoints.contains(&"1.1.1.1".parse().unwrap()));
                }
            }
        }
    }
}

#[test]
fn disabled_dns_fails_before_capture_if_system_egress_cannot_be_discovered() {
    let fail = || {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "DNS discovery unavailable",
        ))
    };
    assert!(prepare_dns_routes(&config("disabled"), false, true, fail).is_err());
    assert!(prepare_dns_routes(&config("disabled"), false, false, fail).is_ok());
}
