use super::*;
use zero_config::{DnsConfig, DnsServerConfig};

fn configured() -> DnsConfig {
    serde_json::from_value(serde_json::json!({
        "servers": {
            "node": {"type":"system"},
            "fallback": {"type":"system"},
            "query": {"type":"udp", "host":"127.0.0.1", "detour":"grpc"}
        },
        "default_server":"query",
        "policy": {"node_server":"node", "node_fallback_servers":["fallback"]}
    }))
    .unwrap()
}

#[test]
fn actual_node_policy_does_not_authorize_or_reject_an_unrelated_default_detour() {
    let config = configured();
    assert!(validate_node_config(Some(&config)).is_ok());
    assert!(
        validate_node_server(&config.servers["query"]).is_err(),
        "the Default detour never becomes a native Node capability"
    );
}

#[test]
fn every_actual_node_primary_and_fallback_requires_a_present_native_backend() {
    for selected in ["node", "fallback"] {
        let mut config = configured();
        let detour = config.servers["query"].clone();
        config.servers.insert(selected.into(), detour);
        assert_eq!(
            validate_node_config(Some(&config)).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        config.servers.remove(selected);
        let error = validate_node_config(Some(&config)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains(selected));
    }
}

#[test]
fn implicit_node_resolution_checks_default_fallback_and_every_dispatch_destination() {
    let mut config = configured();
    config.policy.node_server = None;
    config.policy.node_fallback_servers.clear();
    assert!(validate_node_config(Some(&config)).is_err());
    config.default_server = "node".into();
    assert!(validate_node_config(Some(&config)).is_ok());
    config.policy.fallback_servers = vec!["query".into()];
    assert!(validate_node_config(Some(&config)).is_err());
    config.policy.fallback_servers.clear();
    config.dispatch = vec![zero_config::DnsDispatchRuleConfig {
        condition: zero_config::RuleConditionConfig::Domain {
            values: vec!["actual.test".into()],
        },
        server: "query".into(),
    }];
    assert!(validate_node_config(Some(&config)).is_err());
    config
        .servers
        .insert("query".into(), DnsServerConfig::System);
    assert!(validate_node_config(Some(&config)).is_ok());
    config.servers.remove("query");
    assert_eq!(
        validate_node_config(Some(&config)).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}
