use zero_api::CONFIG_SCHEMA_VERSION;
use zero_config::{ConfigError, RuntimeConfig};

const MINIMAL_CONFIG: &str = r#"{
    "route": {
        "rules": [],
        "final": { "type": "direct" }
    }
}"#;

#[test]
fn omitted_schema_version_is_v1_and_is_exported_explicitly() {
    let config = RuntimeConfig::parse(MINIMAL_CONFIG).expect("parse legacy V1 config");
    assert_eq!(config.schema_version, CONFIG_SCHEMA_VERSION);

    let value = serde_json::to_value(config).expect("serialize config");
    assert_eq!(value["schema_version"], CONFIG_SCHEMA_VERSION);
}

#[test]
fn explicit_v1_schema_is_accepted() {
    let raw = MINIMAL_CONFIG.replacen('{', r#"{"schema_version":1,"#, 1);
    let config = RuntimeConfig::parse(&raw).expect("parse explicit V1 config");
    assert_eq!(config.schema_version, 1);
}

#[test]
fn unknown_schema_version_fails_before_runtime_construction() {
    for version in [0, 2, u32::MAX] {
        let raw = MINIMAL_CONFIG.replacen('{', &format!(r#"{{"schema_version":{version},"#), 1);
        let error = RuntimeConfig::parse(&raw).expect_err("unknown schema must fail");
        assert!(matches!(
            error,
            ConfigError::UnsupportedSchemaVersion {
                found,
                supported: CONFIG_SCHEMA_VERSION,
            } if found == version
        ));
    }

    let future = r#"{
        "schema_version": 2,
        "future_only": true
    }"#;
    assert!(matches!(
        RuntimeConfig::parse(future),
        Err(ConfigError::UnsupportedSchemaVersion {
            found: 2,
            supported: CONFIG_SCHEMA_VERSION,
        })
    ));
}
