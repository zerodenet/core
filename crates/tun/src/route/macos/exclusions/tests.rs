use std::net::IpAddr;

use super::{connected_networks, native_route_outside_capture, requires_host_bypass};

fn prefixes(values: &[&str]) -> Vec<ipnet::IpNet> {
    values.iter().map(|value| value.parse().unwrap()).collect()
}

#[test]
fn wired_default_does_not_override_mobile_or_wifi_dns_routes() {
    let capture = prefixes(&["0.0.0.0/1", "128.0.0.0/1"]);
    // en5, en8 and en0: a different default gateway must not replace either
    // interface's native route to the shared private DNS address.
    let connected = prefixes(&["192.168.34.0/24", "192.168.0.0/24", "192.168.0.0/24"]);
    for server in ["192.168.0.1", "192.168.34.1"] {
        assert!(!requires_host_bypass(
            server.parse().unwrap(),
            &capture,
            &connected
        ));
    }
    // Off-link bootstrap DNS still needs protection from TUN capture.
    assert!(requires_host_bypass(
        "223.5.5.5".parse().unwrap(),
        &capture,
        &connected
    ));
}

#[test]
fn newly_connected_network_removes_the_need_for_an_old_host_bypass() {
    let capture = prefixes(&["0.0.0.0/1", "128.0.0.0/1"]);
    let peer = "192.168.0.1".parse().unwrap();
    assert!(requires_host_bypass(
        peer,
        &capture,
        &prefixes(&["192.168.34.0/24"])
    ));
    assert!(!requires_host_bypass(
        peer,
        &capture,
        &prefixes(&["192.168.34.0/24", "192.168.0.0/24"])
    ));
}

#[test]
fn capture_specificity_and_address_family_are_respected() {
    let connected = prefixes(&["192.168.0.0/24", "2001:db8:1::/64"]);
    let peer = "192.168.0.1".parse().unwrap();
    assert!(requires_host_bypass(
        peer,
        &prefixes(&["192.168.0.0/25"]),
        &connected
    ));
    assert!(!requires_host_bypass(
        peer,
        &prefixes(&["10.0.0.0/8"]),
        &connected
    ));
    let v6 = prefixes(&["::/1", "8000::/1"]);
    assert!(!requires_host_bypass(
        "2001:db8:1::53".parse().unwrap(),
        &v6,
        &connected
    ));
    assert!(requires_host_bypass(
        "2001:db8:2::53".parse().unwrap(),
        &v6,
        &connected
    ));
}

#[test]
fn native_static_and_vpn_routes_are_left_owned_by_the_system() {
    let capture = prefixes(&["0.0.0.0/1", "128.0.0.0/1"]);
    let peer: IpAddr = "192.168.0.1".parse().unwrap();
    for output in [
        "interface: en8\nmask: 255.255.255.0\nflags: <UP,DONE,CLONING,STATIC>\n",
        "interface: utun5\nflags: <UP,HOST,DONE,STATIC>\n",
    ] {
        assert!(native_route_outside_capture(
            output.as_bytes(),
            peer,
            &capture,
            "utun6"
        ));
    }
    for output in [
        "interface: utun6\nflags: <UP,HOST,DONE,STATIC>\n",
        "interface: en5\nmask: 0.0.0.0\nflags: <UP,GATEWAY,DONE>\n",
        "interface: en8\nflags: <UP,HOST,DONE,REJECT>\n",
        "interface: en8\nflags: <UP,HOST,DONE,BLACKHOLE>\n",
        "interface: en8\nflags: <UP,HOST,DONE,IFSCOPE>\n",
        "not a route\n",
    ] {
        assert!(!native_route_outside_capture(
            output.as_bytes(),
            peer,
            &capture,
            "utun6"
        ));
    }
}

#[test]
fn discovers_the_native_macos_loopback_network_without_route_mutation() {
    let connected = connected_networks("zero-nonexistent-test-interface").unwrap();
    assert!(connected
        .iter()
        .any(|network| network.contains(&"127.0.0.1".parse::<IpAddr>().unwrap())));
}

#[test]
fn shortened_bsd_netmasks_are_zero_extended() {
    assert_eq!(
        super::sockaddr_ip(
            libc::AF_INET,
            &[7, libc::AF_INET as u8, 0, 0, 255, 255, 255]
        ),
        Some("255.255.255.0".parse().unwrap())
    );
    assert_eq!(
        super::sockaddr_ip(
            libc::AF_INET6,
            &[
                12,
                libc::AF_INET6 as u8,
                0,
                0,
                0,
                0,
                0,
                0,
                255,
                255,
                255,
                255
            ]
        ),
        Some("ffff:ffff::".parse().unwrap())
    );
}
