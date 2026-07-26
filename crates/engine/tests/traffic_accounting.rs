use std::sync::{Arc, Mutex};

use zero_api::{
    event_type, CallbackEventSink, EventFilter, EventSource, PublishResult, RawApiEvent,
};
use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};
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

#[test]
fn user_direction_traffic_counts_each_relayed_byte_once() {
    let engine = engine();
    let mut session = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Udp,
        ProtocolType::new("test"),
    );
    engine
        .prepare_session(&mut session, "test-in")
        .expect("prepare session");
    let mut handle = engine.track_session(session.id);

    engine.record_session_upload(session.id, 17);
    engine.record_session_download(session.id, 23);

    let active = engine.active_sessions();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].bytes_up, 17);
    assert_eq!(active[0].bytes_down, 23);
    assert_eq!(active[0].inbound_rx_bytes, 17);
    assert_eq!(active[0].outbound_tx_bytes, 17);
    assert_eq!(active[0].outbound_rx_bytes, 23);
    assert_eq!(active[0].inbound_tx_bytes, 23);

    let completed = handle
        .finish(SessionOutcome::DirectRelayed)
        .expect("finish session");
    assert_eq!(completed.bytes_up, 17);
    assert_eq!(completed.bytes_down, 23);
    assert_eq!(engine.stats_snapshot().bytes_up, 17);
    assert_eq!(engine.stats_snapshot().bytes_down, 23);
}

#[test]
fn completed_flow_reaches_durable_sink_before_the_event_log() {
    let base = engine();
    let event_log = base.clone();
    let persisted = Arc::new(Mutex::new(Vec::<RawApiEvent>::new()));
    let persisted_ref = persisted.clone();
    let sink = CallbackEventSink::new("completion-journal", move |event: &RawApiEvent| {
        let retained = event_log.latest(
            usize::MAX,
            EventFilter {
                event_types: vec![event_type::FLOW_COMPLETED.to_owned()],
                ..EventFilter::default()
            },
        )?;
        assert!(
            retained.is_empty(),
            "flow.completed must be persisted before it enters the event log"
        );
        persisted_ref
            .lock()
            .expect("persisted event lock")
            .push(event.clone());
        Ok(PublishResult::delivered())
    });
    let engine = base.with_flow_completion_sink(Arc::new(sink));
    let mut session = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    engine
        .prepare_session(&mut session, "test-in")
        .expect("prepare session");
    let mut handle = engine.track_session(session.id);
    engine.record_session_upload(session.id, 17);
    engine.record_session_download(session.id, 23);
    handle
        .finish(SessionOutcome::DirectRelayed)
        .expect("finish session");

    let persisted = persisted.lock().expect("persisted event lock").clone();
    assert_eq!(persisted.len(), 1);
    let retained = engine
        .latest(
            1,
            EventFilter {
                event_types: vec![event_type::FLOW_COMPLETED.to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("read retained completed flow");
    assert_eq!(retained.len(), 1);
    assert_eq!(persisted[0].event_id, retained[0].event_id);
    assert_eq!(persisted[0].payload, retained[0].payload);
    assert!(persisted[0].sequence.is_none());
    assert!(retained[0].sequence.is_some());
}
