use serde_json::json;
use zero_api::{EndpointConfigurationOrigin, EndpointGetQuery};
use zero_config::RuntimeConfig;
use zero_engine::{EndpointChange, Engine};

fn config() -> RuntimeConfig {
    RuntimeConfig::parse(&json!({"endpoints":[{"tag":"a", "listen":{"address":"127.0.0.1","port":51820},
        "directions":{"inbound":true,"outbound":true}, "protocol":{
        "type":"wireguard","private_key":"01".repeat(32),"addresses":["10.0.0.1/32"],
        "peers":[{"public_key":"02".repeat(32),"endpoint":"127.0.0.1:51821","allowed_ips":["10.0.0.0/24"]}]}}],
        "route":{"rules":[],"final":{"type":"direct"}}}).to_string()).unwrap()
}

fn query() -> EndpointGetQuery {
    EndpointGetQuery {
        endpoint_id: "endpoint:a".into(),
    }
}

#[test]
fn configuration_metadata_keeps_base_directions_separate_from_runtime_override() {
    let engine = Engine::new(config()).unwrap();
    let before = engine.endpoint_snapshot(&query()).unwrap();
    assert_eq!(
        before.configuration.origin,
        EndpointConfigurationOrigin::Canonical
    );
    assert!(!before.configuration.source_file.available);
    assert_eq!(
        before.configuration.source_file.reason.as_deref(),
        Some("source_path_unavailable")
    );
    let changed = engine
        .prepare_endpoint_change(
            &engine.runtime_snapshot(),
            "endpoint:a",
            EndpointChange::Directions(zero_api::EndpointDirections::outbound_only()),
            false,
            None,
        )
        .unwrap();
    engine.restore_staged_snapshot(changed, false).unwrap();
    let after = engine.endpoint_snapshot(&query()).unwrap();
    assert!(!after.allowed.inbound);
    assert!(after.configuration.directions.inbound);
    assert_eq!(after.configuration, before.configuration);

    let mut legacy = config();
    legacy.endpoints.clear();
    let legacy = Engine::new(legacy)
        .unwrap()
        .endpoints_snapshot(&Default::default())
        .endpoints
        .remove(0);
    assert_eq!(
        legacy.configuration.origin,
        EndpointConfigurationOrigin::Legacy
    );
    assert_eq!(
        legacy.configuration.source_file.reason.as_deref(),
        Some("canonical_configuration_required")
    );
}

#[test]
fn source_writability_is_unknown_until_actual_persistence_and_recovers_after_io_failure() {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("source");
    std::fs::write(&parent, b"not a directory").unwrap();
    let path = parent.join("config.json");
    let engine = Engine::new_with_config_path(config(), &path).unwrap();
    let before = engine
        .endpoint_snapshot(&query())
        .unwrap()
        .configuration
        .source_file;
    assert!(before.available);
    assert_eq!(before.writable, None);
    assert_eq!(before.writable_observed_at_unix_ms, None);
    assert!(engine.reload_config(config()).is_err());
    let failed = engine
        .endpoint_snapshot(&query())
        .unwrap()
        .configuration
        .source_file;
    assert!(failed.available);
    assert_eq!(
        failed.writable, None,
        "ordinary I/O failure does not prove permission denial"
    );
    assert_eq!(failed.reason.as_deref(), Some("source_write_failed"));
    assert!(failed.writable_observed_at_unix_ms.is_some());
    std::fs::remove_file(&parent).unwrap();
    std::fs::create_dir(&parent).unwrap();
    engine.reload_config(config()).unwrap();
    let saved = engine
        .endpoint_snapshot(&query())
        .unwrap()
        .configuration
        .source_file;
    assert_eq!(saved.writable, Some(true));
    assert_eq!(saved.reason, None);
    assert!(saved.writable_observed_at_unix_ms.is_some());
    assert!(RuntimeConfig::load_from_path(path).is_ok());
}
