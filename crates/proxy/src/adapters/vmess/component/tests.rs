use super::*;
fn input(extra: &str) -> OutboundConfig {
    zero_config::RuntimeConfig::parse(&format!(
        r#"{{"outbounds":[{{"tag":"native","protocol":{{
        "type":"vmess","server":"127.0.0.1","port":443,
        "id":"11111111-2222-3333-4444-555555555555","cipher":"aes-128-gcm"{extra}
    }}}}],"route":{{"final":{{"type":"direct"}}}}}}"#
    ))
    .unwrap()
    .outbounds
    .remove(0)
}
#[test]
fn resourceful_styles_remain_excluded_and_direct_options_never_discard_them() {
    for extra in [
        r#", "mux_concurrency":2"#,
        r#", "tls":{"ca_cert_path":"not-opened.pem"}"#,
        r#", "tls":{"options":{"ech_config_list":"udp://127.0.0.1:53"}}"#,
        r#", "ws":{"browser_dialer":{}}"#,
    ] {
        let input = input(extra);
        assert!(!native_input(&input), "{extra}");
        assert!(component_options(&input).is_err(), "{extra}");
    }
}
#[test]
fn an_invalid_selected_native_credential_is_an_error_and_never_a_legacy_fallback() {
    let mut input = input("");
    assert!(native_input(&input));
    assert!(component_options(&input).is_ok());
    let OutboundProtocolConfig::Vmess { id, .. } = &mut input.protocol else {
        unreachable!()
    };
    *id = "invalid".into();
    assert!(native_input(&input));
    assert!(component_options(&input).is_err());
}
