use super::*;
fn input(extra: &str) -> OutboundConfig {
    zero_config::RuntimeConfig::parse(&format!(
        r#"{{"outbounds":[{{"tag":"native","protocol":{{
        "type":"vless","server":"127.0.0.1","port":443,
        "id":"11111111-2222-3333-4444-555555555555"{extra}
    }}}}],"route":{{"final":{{"type":"direct"}}}}}}"#
    ))
    .unwrap()
    .outbounds
    .remove(0)
}
#[test]
fn resourceful_styles_are_explicitly_excluded_and_never_dropped_by_direct_options() {
    for extra in [
        r#", "mux_concurrency":2"#,
        r#", "tls":{"ca_cert_path":"not-opened.pem"}"#,
        r#", "tls":{"server_name":"localhost","options":{"ech_config_list":"udp://127.0.0.1:53"}}"#,
        r#", "ws":{"browser_dialer":{}}"#,
    ] {
        let input = input(extra);
        assert!(!native_input(&input), "{extra}");
        assert!(component_options(&input).is_err(), "{extra}");
    }
}
#[test]
fn selected_custom_id_is_parsed_by_protocol_and_invalid_id_never_falls_back() {
    let mut input = input(r#", "encryption":"none""#);
    assert!(native_input(&input));
    assert!(component_options(&input).is_ok());
    let OutboundProtocolConfig::Vless { id, .. } = &mut input.protocol else {
        unreachable!()
    };
    *id = "custom-id".into();
    assert!(native_input(&input));
    assert!(component_options(&input).is_ok());
    let OutboundProtocolConfig::Vless { id, .. } = &mut input.protocol else {
        unreachable!()
    };
    id.clear();
    assert!(native_input(&input));
    assert!(component_options(&input).is_err());
}
#[test]
fn reverse_input_cannot_be_projected_into_an_ordinary_outbound_role() {
    let mut input = input("");
    let OutboundProtocolConfig::Vless { reverse_tag, .. } = &mut input.protocol else {
        unreachable!()
    };
    *reverse_tag = Some("virtual-ingress".into());
    assert!(!native_input(&input));
    assert!(component_options(&input).is_err());
}

#[test]
fn tls_vision_keeps_flow_and_padding_while_invalid_selected_parameters_still_fail() {
    for flow in ["xtls-rprx-vision", "xtls-rprx-vision-udp443"] {
        let mut input = input(&format!(
            r#", "flow":"{flow}","tls":{{"insecure":true}},"testseed":[10,1,20,1]"#
        ));
        assert!(native_input(&input));
        assert!(component_options(&input).is_ok());
        let OutboundProtocolConfig::Vless { testseed, .. } = &mut input.protocol else {
            unreachable!()
        };
        *testseed = vec![10, 0, 20, 1];
        assert!(native_input(&input));
        assert!(
            component_options(&input).is_err(),
            "invalid padding cannot silently choose a legacy path"
        );
    }
}

#[test]
fn reality_library_factory_does_not_activate_production_before_resource_owner_gate() {
    let (_, public) = ::vless::reality::generate_reality_key_pair();
    for flow in [None, Some("xtls-rprx-vision")] {
        let flow = flow
            .map(|flow| format!(",\"flow\":\"{flow}\""))
            .unwrap_or_default();
        let input = input(&format!(
            r#", "reality":{{"public_key":"{public}","short_id":"0123456789abcdef","server_name":"localhost","client_fingerprint":"chrome"}}{flow}"#
        ));
        assert!(
            !native_input(&input),
            "task-bearing REALITY must retain its old production owner"
        );
        let options = component_options(&input).unwrap();
        use zero_platform_tokio::contract::component::ComponentFactory;
        assert_eq!(
            ::vless::component::Factory
                .describe(&options)
                .task_limit
                .unwrap()
                .get(),
            64
        );
    }
}
