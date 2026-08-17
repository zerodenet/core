use std::io;

use super::{parse_default_route, route_add_arguments, route_remove_arguments};

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
