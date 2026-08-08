use zero_api::{
    event_type, ApiEvent, CommandRequest, CommandService, EventFilter, EventSource, FlowListQuery,
    PolicyProbeCommand, PolicyProbeCompletedPayload, PolicyProbeMember, QueryRequest,
    QueryResponse, QueryService,
};
use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{Engine, EngineHandle, ProbeTrigger, ProbeTriggerAck, SessionOutcome};

#[test]
fn policy_probe_command_returns_the_effective_operation_identity() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let engine = Engine::new(config).expect("build engine");
    engine.probe_trigger_registry().register(
        "auto",
        ProbeTrigger::new(|operation_id| ProbeTriggerAck {
            operation_id,
            coalesced: false,
        }),
    );

    let response = engine
        .execute(CommandRequest::PolicyProbe(PolicyProbeCommand {
            policy_tag: "auto".to_owned(),
            operation_id: Some("client-operation".to_owned()),
        }))
        .expect("execute policy probe command");
    let result = response.result.expect("policy probe acknowledgement");
    assert_eq!(result["operation_id"], "client-operation");
    assert_eq!(result["coalesced"], false);
    assert_eq!(result["core_instance_id"], engine.core_instance_id());
    assert_eq!(result["config_revision"], 1);
}

#[test]
fn generated_event_ids_do_not_collide_across_engine_instances() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let first = Engine::new(config.clone()).expect("build first engine");
    let second = Engine::new(config).expect("build second engine");
    let filter = EventFilter {
        event_types: vec![event_type::ENGINE_STARTED.to_owned()],
        ..EventFilter::default()
    };

    let first_event = first
        .latest(1, filter.clone())
        .expect("read first engine event")
        .into_iter()
        .next()
        .expect("first engine started event");
    let second_event = second
        .latest(1, filter)
        .expect("read second engine event")
        .into_iter()
        .next()
        .expect("second engine started event");

    assert_ne!(first_event.event_id, second_event.event_id);
    assert_ne!(first.core_instance_id(), second.core_instance_id());
    for (event, engine) in [(&first_event, &first), (&second_event, &second)] {
        let (epoch, local_id) = event
            .event_id
            .split_once(':')
            .expect("generated event id includes an engine epoch");
        assert_eq!(epoch.len(), 32);
        assert!(epoch.chars().all(|character| character.is_ascii_hexdigit()));
        assert_eq!(local_id, "engine-1");
        assert_eq!(
            event.core_instance_id.as_deref(),
            Some(engine.core_instance_id())
        );
        assert_eq!(event.config_revision, Some(1));
    }

    let replay = second
        .since(
            first.latest_event_sequence(),
            usize::MAX,
            EventFilter::default(),
        )
        .expect("read replay from second engine with first engine cursor");
    assert_eq!(replay.core_instance_id, second.core_instance_id());
    assert_ne!(replay.core_instance_id, first.core_instance_id());
}

#[test]
fn runtime_event_log_capacity_evicts_oldest_events() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": { "event_log_capacity": 2 },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let engine = Engine::new(config.clone()).expect("build engine");

    engine.emit_warning("first", "first retained warning");
    engine.emit_warning("second", "second retained warning");

    let events = engine
        .latest(usize::MAX, EventFilter::default())
        .expect("read retained events");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload["code"], "first");
    assert_eq!(events[1].payload["code"], "second");
}

#[test]
fn live_reload_resizes_the_runtime_event_log() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": { "event_log_capacity": 4 },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let engine = Engine::new(config.clone()).expect("build engine");
    engine.emit_warning("first", "first warning");
    engine.emit_warning("second", "second warning");
    engine.emit_warning("third", "third warning");

    let mut reloaded = config;
    reloaded.runtime.event_log_capacity = 2;
    engine
        .reload_runtime_config(reloaded)
        .expect("reload runtime config");

    let events = engine
        .latest(usize::MAX, EventFilter::default())
        .expect("read resized event log");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload["code"], "third");
    assert_eq!(events[1].event_type, event_type::CONFIG_CHANGED);
    assert_eq!(
        events[1].core_instance_id.as_deref(),
        Some(engine.core_instance_id())
    );
    assert_eq!(events[1].config_revision, Some(2));
    assert_eq!(events[1].payload["config_revision"], 2);
}

#[test]
fn streams_policy_probe_events_from_the_engine_event_log() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let engine = Engine::new(config.clone()).expect("build engine");
    let handle = EngineHandle::new(engine.clone());
    let subscriber = handle
        .subscribe(EventFilter {
            event_types: vec![event_type::POLICY_PROBE_COMPLETED.to_owned()],
            ..EventFilter::default()
        })
        .expect("subscribe to policy probes");

    engine
        .reload_runtime_config(config)
        .expect("activate a newer configuration generation");
    assert_eq!(engine.config_revision(), 2);

    engine.push_policy_probe_completed(
        "auto",
        PolicyProbeCompletedPayload {
            operation_id: "manual-1".to_owned(),
            core_instance_id: String::new(),
            config_revision: 1,
            policy_tag: "auto".to_owned(),
            trigger: "manual".to_owned(),
            url: "http://example.com/".to_owned(),
            started_at_unix_ms: 100,
            completed_at_unix_ms: 125,
            duration_ms: 25,
            terminal_status: "succeeded".to_owned(),
            selected: Some("direct".to_owned()),
            selection: None,
            members: vec![PolicyProbeMember {
                target_tag: "direct".to_owned(),
                healthy: true,
                latency_ms: Some(25),
                error_code: None,
                error: None,
            }],
        },
    );

    let event = subscriber
        .try_recv()
        .expect("receive live policy probe event");
    assert_eq!(event.event_type, event_type::POLICY_PROBE_COMPLETED);
    assert_eq!(event.payload["trigger"], "manual");
    assert_eq!(event.payload["operation_id"], "manual-1");
    assert_eq!(event.payload["core_instance_id"], engine.core_instance_id());
    assert_eq!(event.payload["config_revision"], 1);
    assert_eq!(event.payload["terminal_status"], "succeeded");
    assert_eq!(event.config_revision, Some(1));
    assert_eq!(engine.config_revision(), 2);
    assert_eq!(event.payload["started_at_unix_ms"], 100);
    assert_eq!(event.payload["completed_at_unix_ms"], 125);
    assert_eq!(event.payload["duration_ms"], 25);
    assert_eq!(event.payload["selected"], "direct");
    assert!(event.sequence.is_some());

    let latest = handle
        .latest(
            1,
            EventFilter {
                event_types: vec![event_type::POLICY_PROBE_COMPLETED.to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("read event history");
    assert_eq!(latest, vec![event.clone()]);

    let sequence = event.sequence.expect("event sequence");
    let replay = handle
        .since(
            sequence - 1,
            1,
            EventFilter {
                event_types: vec![event_type::POLICY_PROBE_COMPLETED.to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("replay events after cursor");
    assert_eq!(replay.requested_after, sequence - 1);
    assert_eq!(replay.actual_from, sequence);
    assert!(!replay.has_gap);
    assert_eq!(replay.events, vec![event]);

    let filtered_replay = handle
        .since(
            0,
            1,
            EventFilter {
                event_types: vec![event_type::POLICY_PROBE_COMPLETED.to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("replay filtered events from the beginning");
    assert!(
        !filtered_replay.has_gap,
        "retained non-matching events must not look like an eviction gap"
    );
    assert_eq!(filtered_replay.actual_from, sequence);
}

#[test]
fn engine_event_source_subscribe_is_live_like_engine_handle() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let engine = Engine::new(config).expect("build engine");
    let subscriber = engine
        .subscribe(EventFilter {
            event_types: vec![event_type::ENGINE_WARNING.to_owned()],
            ..EventFilter::default()
        })
        .expect("subscribe directly through Engine");

    engine.emit_warning("test_warning", "live event");

    let event = subscriber.try_recv().expect("receive live engine event");
    assert_eq!(event.event_type, event_type::ENGINE_WARNING);
    assert_eq!(event.payload["code"], "test_warning");
    assert_eq!(event.payload["message"], "live event");
}

#[test]
fn flow_subscription_starts_with_self_contained_active_snapshot() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let engine = Engine::new(config).expect("build engine");
    let handle = EngineHandle::new(engine.clone());
    let mut session = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("socks5"),
    );
    session.source_ip = Some(Address::Ipv4([192, 168, 1, 8]));
    session.source_port = Some(49152);
    session.process_id = Some(4242);
    session.process_name = Some("browser".to_owned());
    session.process_path = Some("/opt/browser".to_owned());
    engine
        .prepare_session(&mut session, "socks-in")
        .expect("session should be admitted");
    engine.record_session_inbound_rx(session.id, 64);
    engine.record_session_outbound_tx(session.id, 64);
    engine.record_session_outbound_rx(session.id, 32);
    engine.record_session_inbound_tx(session.id, 32);

    let subscriber = handle
        .subscribe(EventFilter {
            event_types: vec![event_type::FLOW_ROUTED.to_owned()],
            ..EventFilter::default()
        })
        .expect("subscribe to flow lifecycle");
    let snapshot = subscriber.try_recv().expect("initial flow snapshot");
    assert_eq!(snapshot.event_type, event_type::FLOW_SNAPSHOT);
    assert_eq!(snapshot.payload["records"][0]["flow_id"], "1");
    assert_eq!(snapshot.payload["records"][0]["state"], "active");
    assert_eq!(
        snapshot.payload["records"][0]["source"]["ip"],
        "192.168.1.8"
    );
    assert_eq!(
        snapshot.payload["records"][0]["target"]["host"],
        "example.com"
    );
    assert_eq!(snapshot.payload["records"][0]["traffic"]["bytes_up"], 64);
    assert_eq!(snapshot.payload["records"][0]["traffic"]["bytes_down"], 32);
    assert_eq!(
        snapshot.payload["records"][0]["traffic"]["inbound_rx_bytes"],
        64
    );
    assert_eq!(
        snapshot.payload["records"][0]["traffic"]["outbound_tx_bytes"],
        64
    );

    let QueryResponse::ActiveFlows(active) = handle
        .query(QueryRequest::ActiveFlows(FlowListQuery {
            limit: None,
            filter: Default::default(),
        }))
        .expect("query active flows")
    else {
        panic!("expected active flow query response");
    };
    let active_record = active[0]
        .record
        .as_ref()
        .expect("active query includes canonical record");
    assert_eq!(active_record.revision, 5);
    let source = active_record.source.as_ref().expect("active flow source");
    assert_eq!(source.ip, "192.168.1.8");
    assert_eq!(source.port, Some(49152));
    assert_eq!(source.process_id, Some(4242));
    assert_eq!(source.process_name.as_deref(), Some("browser"));
    assert_eq!(source.process_path.as_deref(), Some("/opt/browser"));
    assert!(active_record.throughput.sampled_at_unix_ms > 0);

    let trace = engine.route_trace_with_inbound(&session.target, None, Some("socks-in"));
    engine.record_session_route(session.id, &trace);
    session.outbound_tag = Some("direct".to_owned());
    engine.set_session_outbound(&session);

    let routed = subscriber.try_recv().expect("flow routed event");
    assert_eq!(routed.event_type, event_type::FLOW_ROUTED);
    assert_eq!(routed.payload["record"]["state"], "active");
    assert_eq!(routed.payload["record"]["route"]["action"], "direct");
    assert_eq!(
        routed.payload["record"]["route"]["selection_chain"][0],
        "direct"
    );
    assert_eq!(
        routed.payload["record"]["path"]["outbound"]["tag"],
        "direct"
    );

    engine
        .finish_session(session.id, SessionOutcome::DirectRelayed)
        .expect("finish observed session");
    let QueryResponse::RecentFlows(recent) = handle
        .query(QueryRequest::RecentFlows(FlowListQuery {
            limit: None,
            filter: Default::default(),
        }))
        .expect("query recent flows")
    else {
        panic!("expected recent flow query response");
    };
    let completed_record = recent[0]
        .record
        .as_ref()
        .expect("recent query includes canonical record");
    assert_eq!(completed_record.state, zero_api::FlowState::Completed);
    assert!(completed_record.revision > active_record.revision);
    assert_eq!(
        completed_record
            .source
            .as_ref()
            .and_then(|source| source.process_path.as_deref()),
        Some("/opt/browser")
    );
    assert_eq!(
        completed_record.throughput.sampled_at_unix_ms,
        completed_record
            .timing
            .ended_at_unix_ms
            .expect("completed timestamp")
    );
    assert!(completed_record.result.is_some());
}

#[test]
fn full_live_queue_does_not_unregister_subscriber() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let engine = Engine::new(config).expect("build engine");
    let handle = EngineHandle::new(engine);
    let subscriber = handle
        .subscribe(EventFilter {
            event_types: vec![event_type::ENGINE_WARNING.to_owned()],
            ..EventFilter::default()
        })
        .expect("subscribe to warnings");

    for index in 0..1_100_u64 {
        handle.emit(ApiEvent::new(
            format!("warning-{index}"),
            event_type::ENGINE_WARNING,
            index,
            serde_json::json!({ "index": index }),
        ));
    }
    while subscriber.try_recv().is_some() {}

    handle.emit(ApiEvent::new(
        "warning-final",
        event_type::ENGINE_WARNING,
        1_101,
        serde_json::json!({ "index": 1_101 }),
    ));
    let final_event = subscriber
        .try_recv()
        .expect("subscriber should remain registered after backpressure");
    assert_eq!(final_event.event_id, "warning-final");
}
