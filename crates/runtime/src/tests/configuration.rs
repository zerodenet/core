use super::PreparedRouting;
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_router::RouteAction;

#[test]
fn one_preparation_retains_relative_resource_for_route_bypass_and_dns() {
    let directory = tempfile::tempdir().unwrap();
    let rules = directory.path().join("domains.txt");
    std::fs::write(&rules, "example.test\n").unwrap();
    let source = directory.path().join("zero.json");
    std::fs::write(&source, r#"{
        "runtime":{"dns":{"default_server":"local","servers":{"local":{"type":"system"}},
            "dispatch":[{"condition":{"type":"rule_set","tag":"domains"},"server":"local"}]}},
        "route":{"rule_sets":[{"tag":"domains","type":"file","path":"domains.txt","format":"domain_list"}],
            "rules":[{"condition":{"type":"rule_set","tag":"domains"},"action":{"type":"direct"}}],
            "bypass":[{"type":"rule_set","tag":"domains"}],"final":{"type":"reject"}}
    }"#).unwrap();
    let config = RuntimeConfig::load_from_path(&source).unwrap();
    let prepared = PreparedRouting::prepare(&config).unwrap();
    // Equality compares actual matcher Arc identity, not just the config tag.
    assert_eq!(
        prepared.router.rules[0].condition,
        prepared.bypass.rules[0].condition
    );
    std::fs::remove_file(&rules).unwrap();
    let address = Address::Domain("example.test".into());
    assert_eq!(
        prepared.router.decide_ref(&address, None),
        &RouteAction::Direct
    );
    assert_eq!(
        prepared.bypass.decide_ref(&address, None),
        &RouteAction::Direct
    );
    assert_eq!(
        prepared
            .dns_dispatch
            .as_ref()
            .unwrap()
            .select("example.test"),
        "local"
    );
    // Structural validation still succeeds, but a new preparation retains the original read error.
    config.validate().unwrap();
    assert!(matches!(
        PreparedRouting::prepare(&config).unwrap_err(),
        zero_config::ConfigError::ReadRuleSet { .. }
    ));
}
