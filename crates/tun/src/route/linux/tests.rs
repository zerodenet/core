use super::parse_default_routes;

#[test]
fn parses_default_routes_from_legacy_and_modern_iproute2_output() {
    let routes = parse_default_routes(
        b"default via 192.0.2.1 dev eth0 proto dhcp metric 100\n\
          default dev wg0 scope link metric 50\n",
    )
    .unwrap();

    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0].dev, "eth0");
    assert_eq!(routes[0].gateway.as_deref(), Some("192.0.2.1"));
    assert_eq!(routes[0].metric, 100);
    assert_eq!(routes[1].dev, "wg0");
    assert_eq!(routes[1].gateway, None);
    assert_eq!(routes[1].metric, 50);
}

#[test]
fn route_without_metric_uses_linux_default_metric() {
    let routes = parse_default_routes(b"default via 192.0.2.1 dev eth0\n").unwrap();
    assert_eq!(routes[0].metric, 0);
}

#[test]
fn rejects_default_route_without_an_interface() {
    let error = parse_default_routes(b"default via 192.0.2.1 metric 100\n").unwrap_err();
    assert!(error.to_string().contains("no interface"));
}
