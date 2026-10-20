use super::*;

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
