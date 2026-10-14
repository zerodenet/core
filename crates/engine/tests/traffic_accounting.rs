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
    assert_eq!(engine.stats_snapshot().bytes_up, 17);
    assert_eq!(engine.stats_snapshot().bytes_down, 23);

    let completed = handle
        .finish(SessionOutcome::DirectRelayed)
        .expect("finish session");
    assert_eq!(completed.bytes_up, 17);
    assert_eq!(completed.bytes_down, 23);
    assert_eq!(engine.stats_snapshot().bytes_up, 17);
    assert_eq!(engine.stats_snapshot().bytes_down, 23);
}

#[test]
fn mirrored_transport_boundaries_do_not_double_count_live_totals() {
    let engine = engine();
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

    engine.record_session_inbound_rx(session.id, 17);
    assert_eq!(engine.stats_snapshot().bytes_up, 17);
    engine.record_session_outbound_tx(session.id, 17);
    assert_eq!(engine.stats_snapshot().bytes_up, 17);

    engine.record_session_outbound_rx(session.id, 23);
    assert_eq!(engine.stats_snapshot().bytes_down, 23);
    engine.record_session_inbound_tx(session.id, 23);
    assert_eq!(engine.stats_snapshot().bytes_down, 23);

    handle
        .finish(SessionOutcome::Failed)
        .expect("finish session");
    let stats = engine.stats_snapshot();
    assert_eq!(stats.bytes_up, 17);
    assert_eq!(stats.bytes_down, 23);
    assert_eq!(stats.failed_sessions, 1);
}

#[test]
fn concurrent_active_flows_contribute_to_one_monotonic_total() {
    let engine = engine();
    let mut first = Session::new(
        0,
        Address::Domain("first.example".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    let mut second = Session::new(
        0,
        Address::Domain("second.example".to_owned()),
        443,
        Network::Udp,
        ProtocolType::new("test"),
    );
    engine
        .prepare_session(&mut first, "test-in")
        .expect("prepare first session");
    engine
        .prepare_session(&mut second, "test-in")
        .expect("prepare second session");
    let mut first_handle = engine.track_session(first.id);
    let mut second_handle = engine.track_session(second.id);

    engine.record_session_upload(first.id, 11);
    engine.record_session_upload(second.id, 13);
    engine.record_session_download(first.id, 17);
    engine.record_session_download(second.id, 19);
    let active = engine.stats_snapshot();
    assert_eq!(active.bytes_up, 24);
    assert_eq!(active.bytes_down, 36);

    first_handle
        .finish(SessionOutcome::Cancelled)
        .expect("finish first session");
    second_handle
        .finish(SessionOutcome::DirectRelayed)
        .expect("finish second session");
    let completed = engine.stats_snapshot();
    assert_eq!(completed.bytes_up, 24);
    assert_eq!(completed.bytes_down, 36);
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

#[test]
fn dropped_subscription_records_and_evicted_samples_do_not_lose_cumulative_accounting() {
    let engine = engine();
    let subscriber = engine.subscribe(EventFilter::default()).unwrap();
    let mut session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        80,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    engine.prepare_session(&mut session, "test-in").unwrap();
    let mut handle = engine.track_session(session.id);
    let before = engine.latest_event_sequence();
    for _ in 0..2048 {
        engine.record_session_upload(session.id, 17);
        engine.record_session_download(session.id, 23);
        engine.push_stats_sampled();
    }
    assert!(
        engine
            .events_since(before, 1, &EventFilter::default())
            .has_gap
    );
    let mut records = 0;
    while subscriber.try_recv().is_some() {
        records += 1;
    }
    assert!(
        records < 2048,
        "fixture must exercise dropped subscription records"
    );
    let live = engine.stats_snapshot();
    assert_eq!(live.bytes_up, 2048 * 17);
    assert_eq!(live.bytes_down, 2048 * 23);
    let traffic = engine
        .traffic_snapshot(&zero_api::TrafficGetQuery {
            scope: zero_api::TrafficScope::Global,
        })
        .unwrap();
    assert_eq!(traffic.planes[0].counters.bytes_up, Some(live.bytes_up));
    assert_eq!(traffic.planes[0].counters.bytes_down, Some(live.bytes_down));
    let completed = handle.finish(SessionOutcome::DirectRelayed).unwrap();
    assert_eq!(completed.bytes_up, live.bytes_up);
    assert_eq!(completed.bytes_down, live.bytes_down);
    assert_eq!(engine.stats_snapshot().bytes_up, live.bytes_up);
}
