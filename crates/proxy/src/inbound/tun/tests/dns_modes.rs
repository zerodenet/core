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

#[test]
fn publishing_host_capture_never_turns_our_dns_peer_into_a_physical_exclusion() {
    use crate::inbound::tun::config::filter_route_exclusions;
    let mut cfg = config("real");
    cfg.runtime
        .dns
        .as_mut()
        .unwrap()
        .servers
        .insert("native".into(), zero_config::DnsServerConfig::System);
    let local = zero_tun::tun_local_addresses(&[(
        "10.19.7.1".parse().unwrap(),
        "255.255.255.0".parse().unwrap(),
    )])
    .unwrap();
    // Startup, DNS publication, repeated notification, link recovery and reload
    // all pass freshly discovered addresses through the same admission fence.
    for observed in [
        vec!["192.168.1.180"],
        vec!["10.19.7.2"],
        vec!["10.19.7.2", "10.19.7.1", "10.0.0.2"],
        vec!["10.19.7.2", "192.168.1.180"],
        vec!["10.19.7.2"],
    ] {
        let (_, mut routes) = prepare_dns_routes(&cfg, true, true, || {
            Ok(observed.iter().map(|ip| ip.parse().unwrap()).collect())
        })
        .unwrap();
        filter_route_exclusions(&mut routes, &local);
        assert!(!routes.contains(&"10.19.7.2".parse().unwrap()));
        assert!(!routes.contains(&"10.19.7.1".parse().unwrap()));
        assert!(routes.contains(&"1.1.1.1".parse().unwrap()));
        for ip in observed
            .into_iter()
            .filter(|ip| !["10.19.7.2", "10.19.7.1"].contains(ip))
        {
            assert!(routes.contains(&ip.parse().unwrap()));
        }
    }
}
