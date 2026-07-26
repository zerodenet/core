use std::sync::mpsc;
use std::time::Duration;

use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session, SessionAuth};
use zero_engine::{Engine, SessionOutcome};

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

fn tracked_session(engine: &Engine, principal_key: &str) -> (u64, zero_engine::SessionHandle) {
    let mut session = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    let mut auth = SessionAuth::new("vless");
    auth.principal_key = Some(principal_key.to_owned());
    session.apply_auth(auth);
    engine
        .prepare_session(&mut session, "vless-in")
        .expect("session should be admitted");
    (session.id, engine.track_session(session.id))
}

#[test]
fn cancelling_principal_signals_matching_sessions_and_keeps_others_running() {
    let engine = engine();
    let (first_id, mut first) = tracked_session(&engine, "account:10001");
    let (second_id, mut second) = tracked_session(&engine, "account:10002");
    let (cancel_tx, cancel_rx) = mpsc::channel();
    assert!(first.register_cancellation(move || {
        cancel_tx.send(()).expect("send cancellation");
    }));

    let cancelled = engine.close_principal_flows("account:10001", "principal_disabled");

    assert_eq!(cancelled, vec![first_id]);
    cancel_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("matching session cancellation");
    assert!(first.is_cancelled());
    assert_eq!(
        first.cancellation_reason().as_deref(),
        Some("principal_disabled")
    );
    assert!(!second.is_cancelled());
    assert_eq!(engine.active_sessions().len(), 2);

    first
        .finish_with_reason(SessionOutcome::Cancelled, first.cancellation_reason())
        .expect("finish cancelled session");
    second
        .finish(SessionOutcome::DirectRelayed)
        .expect("finish unaffected session");
    assert!(engine.active_sessions().is_empty());
    assert_ne!(first_id, second_id);
}

#[test]
fn close_flow_requests_runtime_cancellation_before_session_completion() {
    let engine = engine();
    let (session_id, mut handle) = tracked_session(&engine, "account:10001");
    let (cancel_tx, cancel_rx) = mpsc::channel();
    handle.register_cancellation(move || {
        cancel_tx.send(()).expect("send cancellation");
    });

    engine
        .close_flow(&session_id.to_string())
        .expect("request flow close");

    cancel_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("runtime cancellation");
    assert_eq!(engine.active_sessions().len(), 1);
    handle
        .finish_with_reason(SessionOutcome::Cancelled, handle.cancellation_reason())
        .expect("finish cancelled flow");
    assert!(engine.active_sessions().is_empty());
}

#[test]
fn principal_cancellation_notifies_carriers_without_active_flows() {
    let engine = engine();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let _registration = engine.register_principal_cancellation("account:carrier", move |reason| {
        cancel_tx.send(reason).expect("send carrier cancellation");
    });

    assert!(engine
        .close_principal_flows("account:carrier", "principal_disabled")
        .is_empty());
    assert_eq!(
        cancel_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("carrier cancellation"),
        "principal_disabled"
    );
}

#[test]
fn dropping_principal_cancellation_registration_unsubscribes_carrier() {
    let engine = engine();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let registration = engine.register_principal_cancellation("account:carrier", move |reason| {
        cancel_tx.send(reason).expect("send carrier cancellation");
    });
    drop(registration);

    engine.close_principal_flows("account:carrier", "principal_disabled");
    assert!(cancel_rx.recv_timeout(Duration::from_millis(20)).is_err());
}
