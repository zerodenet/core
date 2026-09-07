use zero_config::RuntimeConfig;

fn config(global: bool, inbound: bool) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "runtime": {"udp": {"enabled": global}},
        "inbounds": [{"tag":"forward", "listen":{"address":"127.0.0.1","port":10000},
            "udp":{"enabled":inbound}, "protocol":{"type":"direct","target":"127.0.0.1","port":443}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}

#[test]
fn direct_udp_validation_matches_build_capability() {
    let result = zero_proxy::validate_config(&config(true, true));
    if cfg!(feature = "managed-datagram-runtime") {
        result.expect("UDP-capable build accepts direct forwarding");
    } else {
        assert!(matches!(
            result,
            Err(zero_engine::EngineError::CompiledFeatureDisabled {
                kind: "inbound UDP",
                protocol: "direct",
                feature: "managed-datagram-runtime",
                ..
            })
        ));
    }
}

#[test]
fn either_udp_policy_allows_tcp_only_on_every_build() {
    for (global, inbound) in [(false, true), (true, false), (false, false)] {
        zero_proxy::validate_config(&config(global, inbound)).unwrap();
    }
}

#[test]
fn direct_uses_existing_udp_defaults_and_has_no_network_selector() {
    let input = serde_json::json!({
        "inbounds":[{"tag":"forward","listen":{"address":"127.0.0.1","port":10000},
            "protocol":{"type":"direct","target":"127.0.0.1","port":443}}],
        "route":{"final":{"type":"direct"}}
    });
    let config = RuntimeConfig::parse(&input.to_string()).unwrap();
    assert!(config.runtime.udp.enabled && config.inbounds[0].udp.enabled);
    let output = serde_json::to_value(config).unwrap();
    assert!(output["inbounds"][0]["protocol"].get("network").is_none());
    let mut obsolete = input;
    obsolete["inbounds"][0]["protocol"]["network"] = "tcp_udp".into();
    assert!(RuntimeConfig::parse(&obsolete.to_string()).is_err());
}
