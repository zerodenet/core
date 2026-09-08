use super::{matches, Route};

#[test]
fn rejects_wrong_gateway_interface_and_dead_routes() {
    for route in [
        r#"[{"dst":"128.0.0.0/1","dev":"other"}]"#,
        r#"[{"dst":"128.0.0.0/1","dev":"tun0","gateway":"192.0.2.1"}]"#,
        r#"[{"dst":"128.0.0.0/1","dev":"tun0","flags":["linkdown"]}]"#,
        r#"[{"dst":"128.0.0.0/1","dev":"tun0","type":"blackhole"}]"#,
    ] {
        let routes: Vec<Route> = serde_json::from_str(route).unwrap();
        assert!(matches(&routes, "128.0.0.0/1", "tun0", None).is_err());
    }
}

#[test]
fn distinguishes_missing_routes_from_intact_capture_and_host_bypasses() {
    assert!(!matches(&[], "128.0.0.0/1", "tun0", None).unwrap());
    let routes =
        serde_json::from_str::<Vec<Route>>(r#"[{"dst":"128.0.0.0/1","dev":"tun0","flags":[]}]"#)
            .unwrap();
    assert!(matches(&routes, "128.0.0.0/1", "tun0", None).unwrap());
    let routes = serde_json::from_str::<Vec<Route>>(
        r#"[{"dst":"8.8.8.8","dev":"eth0","gateway":"192.0.2.1"}]"#,
    )
    .unwrap();
    assert!(matches(&routes, "8.8.8.8/32", "eth0", Some("192.0.2.1")).unwrap());
}
