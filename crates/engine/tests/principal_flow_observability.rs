use zero_api::{
    event_type, EventFilter, EventSource, PrincipalFlowsQuery, QueryRequest, QueryResponse,
    QueryService,
};
use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session, SessionAuth};
use zero_engine::{Engine, SessionOutcome};

#[test]
fn principal_flow_events_and_snapshot_share_authoritative_registry_revisions() {
    let engine = engine();
    let mut first = principal_session("account:42", "one.example");
    let mut second = principal_session("account:42", "two.example");

    engine
        .prepare_session(&mut first, "vless-in")
        .expect("admit first session");
    engine
        .prepare_session(&mut second, "vless-in")
        .expect("admit second session");

    let started = engine
        .latest(
            usize::MAX,
            EventFilter {
                event_types: vec![event_type::FLOW_STARTED.to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("read started events");
    assert_eq!(started.len(), 2);
    assert_eq!(started[0].payload["principal_active_flows"], 1);
    assert_eq!(started[0].payload["session_registry_revision"], 1);
    assert_eq!(started[1].payload["principal_active_flows"], 2);
    assert_eq!(started[1].payload["session_registry_revision"], 2);
    assert!(started
        .iter()
        .all(|event| event.payload["observed_at_unix_ms"].as_u64().is_some()));

    assert_principal_snapshot(&engine, 2, 2, Some(2));

    engine
        .finish_session(first.id, SessionOutcome::DirectRelayed)
        .expect("finish first session");
    assert_principal_snapshot(&engine, 3, 1, Some(3));

    engine
        .finish_session(second.id, SessionOutcome::DirectRelayed)
        .expect("finish second session");
    assert_principal_snapshot(&engine, 4, 0, None);

    let completed = engine
        .latest(
            usize::MAX,
            EventFilter {
                event_types: vec![event_type::FLOW_COMPLETED.to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("read completed events");
    assert_eq!(completed.len(), 2);
    assert_eq!(completed[0].payload["principal_active_flows"], 1);
    assert_eq!(completed[0].payload["session_registry_revision"], 3);
    assert_eq!(completed[1].payload["principal_active_flows"], 0);
    assert_eq!(completed[1].payload["session_registry_revision"], 4);
}

#[test]
fn flow_subscription_snapshot_contains_atomic_principal_reconciliation_state() {
    let engine = engine();
    let mut session = principal_session("account:7", "example.com");
    engine
        .prepare_session(&mut session, "vless-in")
        .expect("admit session");

    let subscriber = engine
        .subscribe(EventFilter {
            event_types: vec![event_type::FLOW_STARTED.to_owned()],
            ..EventFilter::default()
        })
        .expect("subscribe to flow lifecycle");
    let snapshot = subscriber.try_recv().expect("initial flow snapshot");

    assert_eq!(snapshot.event_type, event_type::FLOW_SNAPSHOT);
    assert_eq!(snapshot.payload["session_registry_revision"], 1);
    assert_eq!(
        snapshot.payload["principal_flows"][0]["principal_key"],
        "account:7"
    );
    assert_eq!(snapshot.payload["principal_flows"][0]["active_flows"], 1);
    assert_eq!(
        snapshot.payload["principal_flows"][0]["last_transition_revision"],
        1
    );
}

fn assert_principal_snapshot(
    engine: &Engine,
    expected_revision: u64,
    expected_active: u64,
    expected_last_transition: Option<u64>,
) {
    let QueryResponse::PrincipalFlows(snapshot) = engine
        .query(QueryRequest::PrincipalFlows(PrincipalFlowsQuery))
        .expect("query principal flows")
    else {
        panic!("expected principal flows response");
    };
    assert_eq!(snapshot.core_instance_id, engine.core_instance_id());
    assert_eq!(snapshot.session_registry_revision, expected_revision);
    match expected_last_transition {
        Some(revision) => {
            assert_eq!(snapshot.principals.len(), 1);
            assert_eq!(snapshot.principals[0].principal_key, "account:42");
            assert_eq!(snapshot.principals[0].active_flows, expected_active);
            assert_eq!(snapshot.principals[0].last_transition_revision, revision);
        }
        None => assert!(snapshot.principals.is_empty()),
    }
}

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

fn principal_session(principal_key: &str, host: &str) -> Session {
    let mut session = Session::new(
        0,
        Address::Domain(host.to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    let mut auth = SessionAuth::new("vless");
    auth.principal_key = Some(principal_key.to_owned());
    session.auth = Some(auth);
    session
}
