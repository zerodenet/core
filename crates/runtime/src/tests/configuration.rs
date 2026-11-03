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
    let config = crate::configuration::ConfigurationSource::load_document(&source).unwrap();
    let owner = super::ConfigurationSource::new(&source);
    let prepared = PreparedRouting::prepare_owned(&config, &owner).unwrap();
    assert_eq!(owner.io.original_observation(), (1, 1, false, false));
    // Equality compares actual matcher Arc identity, not just the config tag.
    assert_eq!(
        prepared.router.rules[0].condition,
        prepared.bypass.rules[0].condition
    );
    assert_eq!(
        prepared.dns_dispatch.as_ref(),
        Some(&zero_router::DomainDispatcher::new(
            vec![(
                prepared.router.rules[0].condition.clone(),
                "local".to_owned()
            )],
            "local".to_owned()
        ))
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

#[test]
fn recovery_checks_actual_original_and_candidate_source_across_atomic_commit() {
    use super::ConfigurationSource;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let previous = RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let candidate = RuntimeConfig::parse(r#"{"route":{"final":{"type":"reject"}}}"#).unwrap();
    let previous_bytes = serde_json::to_vec_pretty(&previous).unwrap();
    std::fs::write(&path, &previous_bytes).unwrap();
    let source = ConfigurationSource::new(path.clone());
    source
        .persist_ready(&candidate, "actual-test-owner:1")
        .unwrap();
    assert_eq!(source.observation().successful_writes, 1);
    ConfigurationSource::verify_recovery(&path).unwrap();
    let record = directory.path().join("zero.json.zero-recovery");
    let mut facts: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    assert_eq!(facts["completed_phase"], "persisted");
    assert!(!std::fs::read_to_string(&record).unwrap().contains("route"));
    let backup = directory.path().join("zero.json.zero-last-good");
    assert_eq!(std::fs::read(&backup).unwrap(), previous_bytes);
    // A crash before the atomic replacement leaves the real old file and the
    // Ready record; a crash after it leaves the real candidate named above.
    facts["completed_phase"] = "ready".into();
    std::fs::write(&record, serde_json::to_vec(&facts).unwrap()).unwrap();
    std::fs::write(&path, &previous_bytes).unwrap();
    ConfigurationSource::verify_recovery(&path).unwrap();
    std::fs::write(&path, b"unrelated truncated deployment").unwrap();
    assert_eq!(
        ConfigurationSource::verify_recovery(&path)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[cfg(unix)]
#[test]
fn recovery_backup_and_record_preserve_source_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    std::fs::write(&path, b"{}").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    let source = super::ConfigurationSource::new(path.clone());
    source
        .persist_ready(
            &RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
            "permission-test",
        )
        .unwrap();
    for name in [
        "zero.json",
        "zero.json.zero-last-good",
        "zero.json.zero-recovery",
    ] {
        assert_eq!(
            std::fs::metadata(directory.path().join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
    }
}

#[test]
fn zrs_map_is_handed_to_the_domain_only_after_the_actual_file_close() {
    use zero_rule::{Rule, RuleSet, RuleSetCompiler};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("domains.zrs");
    let (compiled, _) = RuleSetCompiler
        .compile(RuleSet::new(vec![Rule::DomainSuffix(
            "example.test".into(),
        )]))
        .unwrap();
    std::fs::write(&path, zero_rule::zrs::encode(&compiled).unwrap()).unwrap();
    let mut config=RuntimeConfig::parse(r#"{"route":{"rule_sets":[{"tag":"domains","type":"file","path":"domains.zrs","format":"zrs"}],"rules":[{"condition":{"type":"rule_set","tag":"domains"},"action":{"type":"direct"}}],"final":{"type":"reject"}}}"#).unwrap();
    config.source_dir = Some(directory.path().to_owned());
    let source = super::ConfigurationSource::default();
    let prepared = PreparedRouting::prepare_owned(&config, &source).unwrap();
    assert_eq!(source.io.original_observation(), (1, 1, false, false));
    // Atomic pathname replacement preserves the established immutable-map
    // contract. It neither reopens the old descriptor nor proves munmap.
    let replacement = directory.path().join("replacement.zrs");
    std::fs::write(&replacement, b"future unrelated resource").unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    assert_eq!(
        prepared
            .router
            .decide_ref(&Address::Domain("example.test".into()), None),
        &RouteAction::Direct
    );
    assert_eq!(source.io.original_observation(), (1, 1, false, false));
}

#[test]
fn invalid_geoip_bytes_keep_parse_error_after_real_original_close() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("invalid.mmdb"), b"not a database").unwrap();
    let mut config = RuntimeConfig::parse(
        r#"{"route":{"geoip_database":"invalid.mmdb","final":{"type":"direct"}}}"#,
    )
    .unwrap();
    config.source_dir = Some(directory.path().to_owned());
    let source = super::ConfigurationSource::default();
    let error = PreparedRouting::prepare_owned(&config, &source).unwrap_err();
    assert!(error.to_string().contains("invalid geoip database"));
    assert_eq!(source.io.original_observation(), (1, 1, false, false));
    source.ensure_available().unwrap();
}
