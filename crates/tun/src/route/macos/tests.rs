use std::io;

use super::scoped::{
    route_output_has_scoped_flag, scoped_bypass_add_arguments, scoped_bypass_get_arguments,
    scoped_bypass_remove_arguments,
};
use super::{
    gateway_matches_family, loopback_route_add_arguments, loopback_route_get_arguments,
    loopback_route_remove_arguments, parse_default_route, prefix_is_within_loopback,
    route_add_arguments, route_already_exists, route_output_uses_unscoped_loopback,
    route_remove_arguments,
};

#[test]
fn parses_gateway_backed_default_route() {
    let (name, gateway) = parse_default_route(
        b"   route to: default\n\
          destination: default\n\
          gateway: 192.0.2.1\n\
          interface: en0\n",
        "utun7",
    )
    .unwrap();

    assert_eq!(name, "en0");
    assert_eq!(gateway.as_deref(), Some("192.0.2.1"));
}

#[test]
fn parses_on_link_default_route_without_gateway() {
    let (name, gateway) = parse_default_route(b"interface: ppp0\n", "utun7").unwrap();

    assert_eq!(name, "ppp0");
    assert_eq!(gateway, None);
}

#[test]
fn rejects_the_zero_tun_interface_as_default_egress() {
    let error =
        parse_default_route(b"gateway: 10.66.0.2\ninterface: utun7\n", "utun7").unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

#[test]
fn rejects_non_utf8_route_output() {
    let error = parse_default_route(&[0xff], "utun7").unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn ipv4_split_route_uses_the_utun_point_to_point_gateway() {
    assert_eq!(
        route_add_arguments(false, "utun7", Some("10.66.0.2"), "0.0.0.0/1"),
        ["-n", "add", "-inet", "0.0.0.0/1", "10.66.0.2"]
    );
}

#[test]
fn ipv6_split_route_remains_interface_backed() {
    assert_eq!(
        route_add_arguments(true, "utun7", None, "::/1"),
        ["-n", "add", "-inet6", "::/1", "-interface", "utun7"]
    );
}

#[test]
fn split_route_delete_uses_only_the_route_key() {
    assert_eq!(
        route_remove_arguments(false, "128.0.0.0/1"),
        ["-n", "delete", "-inet", "128.0.0.0/1"]
    );
}

#[test]
fn scoped_bypass_routes_use_the_physical_interface_and_gateway() {
    assert_eq!(
        scoped_bypass_get_arguments(false, "en0"),
        ["-n", "get", "-inet", "-ifscope", "en0", "default"]
    );
    assert_eq!(
        scoped_bypass_add_arguments(false, "en0", Some("192.168.64.1")),
        [
            "-n",
            "add",
            "-inet",
            "-ifscope",
            "en0",
            "default",
            "192.168.64.1"
        ]
    );
    assert_eq!(
        scoped_bypass_remove_arguments(false, "en0"),
        ["-n", "delete", "-inet", "-ifscope", "en0", "default"]
    );
    assert_eq!(
        scoped_bypass_add_arguments(true, "en0", None),
        [
            "-n",
            "add",
            "-inet6",
            "-ifscope",
            "en0",
            "default",
            "-interface",
            "en0"
        ]
    );
}

#[test]
fn detects_only_interface_scoped_route_output() {
    assert!(route_output_has_scoped_flag(
        b"interface: en0\nflags: <UP,GATEWAY,DONE,STATIC,IFSCOPE>\n"
    ));
    assert!(!route_output_has_scoped_flag(
        b"interface: en0\nflags: <UP,GATEWAY,DONE,STATIC,GLOBAL>\n"
    ));
}

#[test]
fn scoped_bypass_skips_cross_family_fallback_gateways() {
    assert!(gateway_matches_family(false, Some("192.168.64.1")));
    assert!(gateway_matches_family(true, Some("fe80::1%en0")));
    assert!(!gateway_matches_family(false, Some("fe80::1%en0")));
    assert!(!gateway_matches_family(true, Some("192.168.64.1")));
}

#[test]
fn loopback_bypass_routes_are_explicitly_unscoped_and_interface_owned() {
    assert_eq!(
        loopback_route_get_arguments(false),
        ["-n", "get", "-inet", "127.0.0.1"]
    );
    assert_eq!(
        loopback_route_add_arguments(false),
        ["-n", "add", "-inet", "127.0.0.0/8", "-interface", "lo0"]
    );
    assert_eq!(
        loopback_route_remove_arguments(false, "127.0.0.0/8", "lo0"),
        ["-n", "delete", "-inet", "127.0.0.0/8", "-interface", "lo0"]
    );
    assert_eq!(
        loopback_route_add_arguments(true),
        ["-n", "add", "-inet6", "::1/128", "-interface", "lo0"]
    );
}

#[test]
fn loopback_route_detection_rejects_scoped_and_non_loopback_routes() {
    assert!(route_output_uses_unscoped_loopback(
        b"interface: lo0\nflags: <UP,DONE,STATIC,LOCAL>\n"
    ));
    assert!(!route_output_uses_unscoped_loopback(
        b"interface: lo0\nflags: <UP,DONE,STATIC,IFSCOPE>\n"
    ));
    assert!(!route_output_uses_unscoped_loopback(
        b"interface: utun7\nflags: <UP,DONE,STATIC>\n"
    ));
}

#[test]
fn only_an_existing_route_is_treated_as_an_owned_route_collision() {
    assert!(route_already_exists(&io::Error::other(
        "route: writing to routing socket: File exists"
    )));
    assert!(!route_already_exists(&io::Error::new(
        io::ErrorKind::PermissionDenied,
        "not permitted"
    )));
}

#[test]
fn capture_entries_wholly_inside_loopback_are_not_installed() {
    assert!(prefix_is_within_loopback("127.0.0.0/8".parse().unwrap()));
    assert!(prefix_is_within_loopback("127.0.0.1/32".parse().unwrap()));
    assert!(prefix_is_within_loopback("::1/128".parse().unwrap()));
    assert!(!prefix_is_within_loopback("0.0.0.0/1".parse().unwrap()));
    assert!(!prefix_is_within_loopback("::/1".parse().unwrap()));
}
