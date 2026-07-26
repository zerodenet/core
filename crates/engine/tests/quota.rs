use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session, SessionAuth};
use zero_engine::{
    inspect_principal_quota_state, Engine, EngineError, PrincipalQuotaStateStatus, SessionHandle,
    SessionOutcome,
};

fn engine() -> Engine {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    Engine::new(config).expect("build engine")
}

fn admit(
    engine: &Engine,
    revision: u64,
    quota_remaining_bytes: u64,
) -> Result<(u64, SessionHandle), EngineError> {
    let mut session = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    let mut auth = SessionAuth::new("vless");
    auth.principal_key = Some("account:1".to_owned());
    auth.policy_revision = Some(revision);
    auth.quota_remaining_bytes = Some(quota_remaining_bytes);
    session.apply_auth(auth);
    engine.prepare_session(&mut session, "vless-in")?;
    Ok((session.id, engine.track_session(session.id)))
}

#[test]
fn quota_is_shared_across_concurrent_sessions_and_cancels_the_principal() {
    let engine = engine();
    let (first_id, mut first) = admit(&engine, 1, 100).unwrap();
    let (second_id, mut second) = admit(&engine, 1, 100).unwrap();

    engine.record_session_inbound_rx(first_id, 60);
    assert!(!first.is_cancelled());
    assert!(!second.is_cancelled());

    engine.record_session_inbound_tx(second_id, 40);
    assert!(first.is_cancelled());
    assert!(second.is_cancelled());
    assert_eq!(
        first.cancellation_reason().as_deref(),
        Some("quota_exhausted")
    );
    assert_eq!(
        second.cancellation_reason().as_deref(),
        Some("quota_exhausted")
    );

    first
        .finish_with_reason(SessionOutcome::Cancelled, first.cancellation_reason())
        .unwrap();
    second
        .finish_with_reason(SessionOutcome::Cancelled, second.cancellation_reason())
        .unwrap();
    assert!(matches!(
        admit(&engine, 1, 100),
        Err(EngineError::AdmissionDenied { .. })
    ));

    let (_, mut renewed) = admit(&engine, 2, 50).expect("new revision resets quota");
    renewed.finish(SessionOutcome::DirectRelayed).unwrap();
}

#[test]
fn zero_remaining_quota_is_denied_before_flow_registration() {
    let engine = engine();

    assert!(matches!(
        admit(&engine, 1, 0),
        Err(EngineError::AdmissionDenied { .. })
    ));
    assert!(engine.active_sessions().is_empty());
}

#[test]
fn stale_policy_revision_is_rejected_after_config_reload() {
    let config_for_revision = |revision| {
        RuntimeConfig::parse(&format!(
            r#"{{
                "inbounds": [{{
                    "tag": "vless-in",
                    "listen": {{ "address": "127.0.0.1", "port": 1080 }},
                    "protocol": {{
                        "type": "vless",
                        "users": [{{
                            "id": "11111111-1111-1111-1111-111111111111",
                            "principal_key": "account:1",
                            "policy_revision": {revision}
                        }}]
                    }}
                }}],
                "outbounds": [{{ "tag": "direct", "protocol": {{ "type": "direct" }} }}],
                "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
            }}"#
        ))
        .expect("parse versioned config")
    };
    let engine = Engine::new(config_for_revision(1)).unwrap();
    engine.reload_config(config_for_revision(2)).unwrap();

    assert!(matches!(
        admit(&engine, 1, 100),
        Err(EngineError::AdmissionDenied { .. })
    ));
    let (_, mut current) = admit(&engine, 2, 100).unwrap();
    current.finish(SessionOutcome::DirectRelayed).unwrap();
}

#[test]
fn deleted_principal_rejects_an_in_flight_stale_handshake() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [{
                "tag": "vless-in",
                "listen": { "address": "127.0.0.1", "port": 1080 },
                "protocol": {
                    "type": "vless",
                    "users": [{
                        "id": "11111111-1111-1111-1111-111111111111",
                        "principal_key": "account:1",
                        "policy_revision": 1
                    }]
                }
            }],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse versioned config");
    let empty = RuntimeConfig::parse(
        r#"{
            "inbounds": [{
                "tag": "vless-in",
                "listen": { "address": "127.0.0.1", "port": 1080 },
                "protocol": { "type": "vless", "users": [] }
            }],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config without the principal");
    let engine = Engine::new(config).unwrap();

    engine.reload_config(empty).unwrap();

    assert!(matches!(
        admit(&engine, 1, 100),
        Err(EngineError::AdmissionDenied { .. })
    ));
    assert!(engine.active_sessions().is_empty());
}

#[test]
fn quota_balance_recovers_after_engine_restart() {
    let directory = tempfile::tempdir().unwrap();
    let state_path = directory.path().join("quota-state.json");
    let mut config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .unwrap();
    config.runtime.principal_quota_state_path = Some(state_path.display().to_string());

    let first = Engine::new(config.clone()).unwrap();
    let (session_id, mut handle) = admit(&first, 7, 100).unwrap();
    first.record_session_inbound_rx(session_id, 60);
    handle.finish(SessionOutcome::DirectRelayed).unwrap();
    drop(handle);
    drop(first);
    assert!(state_path.exists());

    let restarted = Engine::new(config).unwrap();
    let (session_id, mut handle) = admit(&restarted, 7, 100).unwrap();
    restarted.record_session_inbound_tx(session_id, 40);
    assert!(handle.is_cancelled());
    handle
        .finish_with_reason(SessionOutcome::Cancelled, handle.cancellation_reason())
        .unwrap();
    assert!(matches!(
        admit(&restarted, 7, 100),
        Err(EngineError::AdmissionDenied { .. })
    ));
}

#[test]
fn quota_state_rejects_a_second_live_engine_owner() {
    let directory = tempfile::tempdir().unwrap();
    let state_path = directory.path().join("quota-state.json");
    let mut config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .unwrap();
    config.runtime.principal_quota_state_path = Some(state_path.display().to_string());

    let first = Engine::new(config.clone()).expect("first quota owner");
    let error = match Engine::new(config.clone()) {
        Err(error) => error,
        Ok(_) => panic!("second quota owner must fail"),
    };
    assert_eq!(error.code(), "io");
    assert!(error.to_string().contains("already owned"));

    drop(first);
    Engine::new(config).expect("quota state must be reusable after owner exits");
}

#[test]
fn invalid_quota_recovery_state_fails_engine_startup() {
    let directory = tempfile::tempdir().unwrap();
    let state_path = directory.path().join("quota-state.json");
    std::fs::write(&state_path, b"not-json").unwrap();
    let mut config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .unwrap();
    config.runtime.principal_quota_state_path = Some(state_path.display().to_string());

    assert!(matches!(Engine::new(config), Err(EngineError::Io(_))));
}

#[test]
fn quota_state_inspection_is_read_only_and_rejects_duplicate_principals() {
    let directory = tempfile::tempdir().unwrap();
    let state_path = directory.path().join("quota-state.json");
    let mut config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .unwrap();
    config.runtime.principal_quota_state_path = Some(state_path.display().to_string());

    let missing = inspect_principal_quota_state(&config).expect("quota report");
    assert_eq!(missing.status, PrincipalQuotaStateStatus::Missing);
    assert!(missing.is_compatible());

    let duplicate = br#"{
        "version":1,
        "balances":[
            {
                "key":{
                    "principal_key":"account:1",
                    "policy_revision":1,
                    "initial_bytes":100
                },
                "remaining_bytes":80
            },
            {
                "key":{
                    "principal_key":"account:1",
                    "policy_revision":2,
                    "initial_bytes":200
                },
                "remaining_bytes":150
            }
        ]
    }"#;
    std::fs::write(&state_path, duplicate).unwrap();
    let report = inspect_principal_quota_state(&config).expect("quota report");
    assert_eq!(report.status, PrincipalQuotaStateStatus::Incompatible);
    assert!(report
        .error
        .as_deref()
        .is_some_and(|error| error.contains("duplicate principal")));
    assert_eq!(std::fs::read(&state_path).unwrap(), duplicate);
    assert!(matches!(Engine::new(config), Err(EngineError::Io(_))));
}
