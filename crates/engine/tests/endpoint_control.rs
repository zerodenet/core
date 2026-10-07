use serde_json::json;
use zero_api::{ApiErrorCode, EndpointGetQuery, EndpointStateSource};
use zero_config::RuntimeConfig;
use zero_engine::{EndpointAdmission, EndpointChange, Engine};

fn config() -> RuntimeConfig {
    RuntimeConfig::parse(&json!({"endpoints":[{"tag":"a","protocol":{
        "type":"wireguard","private_key":"01".repeat(32),"addresses":["10.0.0.1/32"],
        "peers":[{"public_key":"02".repeat(32),"endpoint":"127.0.0.1:51821","allowed_ips":["10.0.0.0/24"]}]}}],
        "route":{"rules":[],"final":{"type":"route","outbound":"a"}}}).to_string()).unwrap()
}

fn query() -> EndpointGetQuery {
    EndpointGetQuery {
        endpoint_id: "endpoint:a".into(),
    }
}

#[test]
fn override_survives_reload_and_clear_restores_config_intent() {
    let engine = Engine::new(config()).unwrap();
    let original = engine.runtime_snapshot();
    let revision = engine.endpoint_snapshot(&query()).unwrap().intent_revision;
    let stopped = engine
        .prepare_endpoint_change(
            &original,
            "endpoint:a",
            EndpointChange::Enabled(false),
            false,
            Some(revision),
        )
        .unwrap();
    engine.restore_staged_snapshot(stopped, false).unwrap();
    assert!(
        engine.config().endpoints[0].enabled,
        "runtime override is not configuration"
    );
    assert!(EndpointAdmission::from_snapshot(&engine.runtime_snapshot())
        .outbound_denial("a")
        .is_some());
    let mut reloaded = config();
    reloaded.runtime.event_log_capacity += 1;
    engine.reload_runtime_config(reloaded).unwrap();
    let endpoint = engine.endpoint_snapshot(&query()).unwrap();
    assert!(!endpoint.enabled);
    assert_eq!(endpoint.state_source, EndpointStateSource::RuntimeOverride);
    let clear = engine
        .prepare_endpoint_change(
            &engine.runtime_snapshot(),
            "endpoint:a",
            EndpointChange::ClearOverrides,
            false,
            None,
        )
        .unwrap();
    engine.restore_staged_snapshot(clear, false).unwrap();
    assert!(engine.endpoint_snapshot(&query()).unwrap().enabled);
    assert_eq!(
        engine.endpoint_snapshot(&query()).unwrap().state_source,
        EndpointStateSource::Config
    );
}

#[test]
fn revisions_are_conditional_and_repeat_commands_are_idempotent() {
    let engine = Engine::new(config()).unwrap();
    let old = engine.runtime_snapshot();
    let stopped = engine
        .prepare_endpoint_change(
            &old,
            "endpoint:a",
            EndpointChange::Enabled(false),
            false,
            None,
        )
        .unwrap();
    engine
        .restore_staged_snapshot(stopped.clone(), false)
        .unwrap();
    let repeated = engine
        .prepare_endpoint_change(
            &stopped,
            "endpoint:a",
            EndpointChange::Enabled(false),
            false,
            None,
        )
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(&stopped, &repeated));
    assert_eq!(
        engine
            .prepare_endpoint_change(
                &stopped,
                "endpoint:a",
                EndpointChange::Enabled(true),
                false,
                Some(1)
            )
            .unwrap_err()
            .code,
        ApiErrorCode::Conflict
    );
    assert_eq!(
        engine
            .prepare_endpoint_change(&stopped, "endpoint:a", EndpointChange::Restart, false, None)
            .unwrap_err()
            .code,
        ApiErrorCode::InvalidArgument
    );
}

#[test]
fn removal_clears_override_and_recreated_id_gets_a_new_revision() {
    let engine = Engine::new(config()).unwrap();
    let stop = engine
        .prepare_endpoint_change(
            &engine.runtime_snapshot(),
            "endpoint:a",
            EndpointChange::Enabled(false),
            false,
            None,
        )
        .unwrap();
    engine.restore_staged_snapshot(stop, false).unwrap();
    let previous_revision = engine.endpoint_snapshot(&query()).unwrap().intent_revision;
    engine
        .reload_runtime_config(
            RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
    engine.reload_runtime_config(config()).unwrap();
    let next = engine.endpoint_snapshot(&query()).unwrap();
    assert!(next.enabled);
    assert_eq!(next.state_source, EndpointStateSource::Config);
    assert!(next.intent_revision > previous_revision);
}

#[test]
fn source_file_change_clears_override_and_rollback_restores_policy() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let engine = Engine::new_with_config_path(config(), &path).unwrap();
    let stop = engine
        .prepare_endpoint_change(
            &engine.runtime_snapshot(),
            "endpoint:a",
            EndpointChange::Enabled(false),
            false,
            None,
        )
        .unwrap();
    engine.restore_staged_snapshot(stop, false).unwrap();
    let previous = engine.runtime_snapshot();
    let next = engine
        .prepare_endpoint_change(
            &previous,
            "endpoint:a",
            EndpointChange::Enabled(true),
            true,
            None,
        )
        .unwrap();
    engine.restore_staged_snapshot(next, true).unwrap();
    assert!(engine.endpoint_snapshot(&query()).unwrap().enabled);
    assert_eq!(
        engine.endpoint_snapshot(&query()).unwrap().state_source,
        EndpointStateSource::Config
    );
    let revision = engine.endpoint_snapshot(&query()).unwrap().intent_revision;
    let repeated = engine
        .prepare_endpoint_change(
            &engine.runtime_snapshot(),
            "endpoint:a",
            EndpointChange::Enabled(true),
            true,
            Some(revision),
        )
        .unwrap();
    assert_eq!(
        engine
            .endpoint_snapshot_in(&repeated, &query())
            .unwrap()
            .intent_revision,
        revision,
        "writing an identical source intent does not create a new intent revision"
    );
    engine.restore_staged_snapshot(previous, true).unwrap();
    assert!(!engine.endpoint_snapshot(&query()).unwrap().enabled);
    assert!(
        RuntimeConfig::load_from_path(path).unwrap().endpoints[0].enabled,
        "rollback writes base configuration, never a runtime override"
    );
}

#[test]
fn discarded_candidate_revision_is_not_reused_after_rollback() {
    let engine = Engine::new(config()).unwrap();
    let previous = engine.runtime_snapshot();
    let candidate = engine
        .prepare_endpoint_change(
            &previous,
            "endpoint:a",
            EndpointChange::Enabled(false),
            false,
            None,
        )
        .unwrap();
    let discarded = engine
        .endpoint_snapshot_in(&candidate, &query())
        .unwrap()
        .intent_revision;
    engine
        .restore_staged_snapshot(previous.clone(), false)
        .unwrap();
    let retry = engine
        .prepare_endpoint_change(
            &previous,
            "endpoint:a",
            EndpointChange::Enabled(false),
            false,
            None,
        )
        .unwrap();
    assert!(
        engine
            .endpoint_snapshot_in(&retry, &query())
            .unwrap()
            .intent_revision
            > discarded
    );
}

#[test]
fn packet_pin_cleanup_waits_for_confirmed_role_revocation() {
    use zero_api::EndpointRuntimeState;
    let engine = Engine::new(config()).unwrap();
    let mut observed = engine.endpoint_snapshot(&query()).unwrap();
    observed.state = EndpointRuntimeState::Running;
    engine.record_endpoint_runtime_state(&observed);
    let candidate = engine
        .prepare_endpoint_change(
            &engine.runtime_snapshot(),
            "endpoint:a",
            EndpointChange::Directions(zero_api::EndpointDirections::default()),
            false,
            None,
        )
        .unwrap();
    engine.restore_staged_snapshot(candidate, false).unwrap();
    assert!(
        engine.confirmed_endpoint_packet_revocations().1.is_empty(),
        "staged candidate must not retire pins"
    );
    observed = engine.endpoint_snapshot(&query()).unwrap();
    observed.state = EndpointRuntimeState::Running;
    engine.record_endpoint_runtime_state(&observed);
    assert!(engine
        .confirmed_endpoint_packet_revocations()
        .1
        .contains("a"));
}

#[test]
fn actual_component_replacement_advances_generation_while_recovery_and_policy_do_not() {
    let engine = Engine::new(config()).unwrap();
    let query = EndpointGetQuery {
        endpoint_id: "endpoint:a".into(),
    };
    let mut endpoint = engine.endpoint_snapshot(&query).unwrap();
    endpoint.state = zero_api::EndpointRuntimeState::Running;
    engine.record_endpoint_device_state(&endpoint, vec![1, 2]);
    let first = engine.endpoint_snapshot(&query).unwrap();
    engine.record_endpoint_device_state(&endpoint, vec![1, 2]);
    assert_eq!(
        engine.endpoint_snapshot(&query).unwrap().generation,
        first.generation
    );
    engine.record_endpoint_recovery(
        &query.endpoint_id,
        zero_api::EndpointRecovery {
            network_generation: 7,
            phase: zero_api::EndpointRecoveryPhase::Retrying,
            observed_at_unix_ms: 1,
            retry_after_ms: Some(1000),
            error: Some("offline".into()),
        },
    );
    let retry = engine.endpoint_snapshot(&query).unwrap();
    assert_eq!(retry.generation, first.generation);
    assert_eq!(
        retry.recovery.unwrap().phase,
        zero_api::EndpointRecoveryPhase::Retrying
    );
    engine.record_endpoint_device_state(&endpoint, vec![1, 3]);
    assert!(engine.endpoint_snapshot(&query).unwrap().generation > first.generation);
}
