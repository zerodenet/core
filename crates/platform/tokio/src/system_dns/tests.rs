#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::net::IpAddr;

use super::usable_upstream;

#[test]
fn upstream_filter_rejects_local_and_non_routable_addresses() {
    for address in [
        "0.0.0.0",
        "127.0.0.1",
        "169.254.1.1",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "fe80::1",
        "ff02::1",
    ] {
        assert!(!usable_upstream(address.parse().unwrap()));
    }
    assert!(usable_upstream("192.168.1.1".parse().unwrap()));
    assert!(usable_upstream("2001:4860:4860::8888".parse().unwrap()));
}

#[cfg(windows)]
#[test]
#[ignore = "requires an active non-local Windows DNS configuration"]
fn discovers_current_windows_dns_upstreams() {
    let servers = super::system_dns_servers().expect("discover Windows DNS upstreams");
    assert!(!servers.is_empty());
    assert!(servers.into_iter().all(usable_upstream));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn resolver_text_parser_accepts_resolv_conf_and_scutil_shapes() {
    let parsed = super::parse_nameserver_lines(
        "nameserver 1.1.1.1\n\
         nameserver 2606:4700:4700::1111 # comment\n\
         nameserver[0] : 192.0.2.53\n\
         nameserver[1] : fe80::53%en0\n",
    );
    assert_eq!(
        parsed,
        vec![
            "1.1.1.1".parse::<IpAddr>().unwrap(),
            "2606:4700:4700::1111".parse().unwrap(),
            "192.0.2.53".parse().unwrap(),
            "fe80::53".parse().unwrap(),
        ]
    );
}
