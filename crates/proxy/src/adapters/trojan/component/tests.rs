use super::*;
fn input(extra: &str) -> OutboundConfig {
    zero_config::RuntimeConfig::parse(&format!(r#"{{"outbounds":[{{"tag":"native","protocol":{{"type":"trojan","server":"127.0.0.1","port":443,"password":"test"{extra}}}}}],"route":{{"final":{{"type":"direct"}}}}}}"#)).unwrap().outbounds.remove(0)
}
#[test]
fn grpc_retains_its_existing_driver_owner_while_ordinary_tls_and_websocket_remain_native() {
    let registry = crate::register::component_registry();
    assert!(registry.has_factory("trojan"));
    for extra in [
        r#", "grpc":{"service_names":["native/Tun"],"multi_mode":false}"#,
        r#", "grpc":{"service_names":["native/Tun"],"multi_mode":true}"#,
    ] {
        let input = input(extra);
        assert!(!native_input(&input), "{extra}");
        assert!(
            !registry.has_factory_for("trojan", &input),
            "unmigrated configuration retains its legacy owner: {extra}"
        );
        assert!(component_options(&input).is_err(), "{extra}");
    }
    for extra in ["", r#", "ws":{"path":"/native"}"#] {
        let input = input(extra);
        assert!(native_input(&input), "{extra}");
        assert!(registry.has_factory_for("trojan", &input), "{extra}");
        assert!(component_options(&input).is_ok(), "{extra}");
    }
}

#[test]
fn browser_resource_options_are_never_silently_projected_into_ordinary_websocket() {
    let config = zero_config::RuntimeConfig::parse(
        r#"{
        "outbounds":[{"tag":"browser","protocol":{
            "type":"trojan","server":"127.0.0.1","port":443,"password":"test",
            "ws":{"path":"/native","browser_dialer":{}}
        }}],"route":{"final":{"type":"direct"}}
    }"#,
    )
    .unwrap();
    let input = &config.outbounds[0];
    assert!(!native_input(input));
    assert!(component_options(input).is_err());
}
