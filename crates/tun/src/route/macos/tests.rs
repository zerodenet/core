use std::io;

use super::parse_default_route;

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
