use super::*;
use zero_api::QueryService;
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
fn engine() -> Engine {
    Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
fn route(engine: &Engine, tag: &str) -> PacketRouteLease {
    engine
        .register_packet_route(PacketRouteSnapshot {
            route_id: String::new(),
            core_instance_id: String::new(),
            config_revision: 0,
            inbound_tag: "tun-雪".into(),
            outbound_tag: tag.into(),
            endpoints: vec![],
            source: "10.0.0.2".into(),
            destination: "203.0.113.7".into(),
            ip_protocol: 17,
            translated: false,
            started_at_unix_ms: 0,
            state: PacketRouteState::Active,
        })
        .unwrap()
}
fn book() -> Arc<ControlBudget> {
    ControlBudget::new(ControlBudgetLimits::from_runtime(&Default::default()))
}
#[test]
fn original_packet_route_wire_filters_pagination_and_missing_error_are_preserved() {
    let engine = engine();
    let a = route(&engine, "out-雪");
    let _b = route(&engine, "other");
    for request in [
        QueryRequest::PacketRoutes(Default::default()),
        QueryRequest::PacketRoutes(PacketRouteListQuery {
            limit: Some(1),
            ..Default::default()
        }),
        QueryRequest::PacketRoutes(PacketRouteListQuery {
            offset: 1,
            limit: Some(1),
            ..Default::default()
        }),
        QueryRequest::PacketRoutes(PacketRouteListQuery {
            outbound_tag: Some("out-雪".into()),
            ..Default::default()
        }),
        QueryRequest::PacketRoute(PacketRouteGetQuery {
            route_id: a.control.snapshot.route_id.clone(),
        }),
    ] {
        let mut expected = serde_json::to_value(engine.query(request.clone()).unwrap()).unwrap();
        let book = book();
        let mut credit = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
        {
            let guard = engine.control_runtime_query(&request).unwrap();
            let view = guard.freeze(&mut credit).unwrap().unwrap();
            let actual = serde_json::to_value(&view).unwrap();
            if let Some(page) = expected.get_mut("packet_routes") {
                page["observed_at_unix_ms"] =
                    actual["packet_routes"]["observed_at_unix_ms"].clone();
            }
            assert_eq!(actual, expected);
            assert_eq!(
                serde_json::to_value(view.body()).unwrap(),
                actual.as_object().unwrap().values().next().unwrap().clone()
            );
        }
        assert!(book.usage().response_bytes > 0);
        drop(credit);
        assert_eq!(book.usage().response_bytes, 0);
    }
    let request = QueryRequest::PacketRoute(PacketRouteGetQuery {
        route_id: "missing-雪".into(),
    });
    let book = book();
    let mut credit = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let guard = engine.control_runtime_query(&request).unwrap();
    let missing = guard.freeze(&mut credit).unwrap().err().unwrap();
    assert_eq!(
        missing.message().to_string(),
        "packet route no longer active"
    );
    assert_eq!(
        missing.message_len(),
        Some(missing.message().to_string().len())
    );
}
#[test]
fn frozen_state_and_original_fields_survive_real_lease_return_without_forging_response_receipt() {
    let engine = engine();
    let lease = route(&engine, "actual");
    let control = lease.control();
    control.responses_started();
    let request = QueryRequest::PacketRoutes(Default::default());
    let book = book();
    let mut credit = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.packet_query(&request);
        let view = guard.freeze(&mut credit).unwrap();
        let originals = &view.rows;
        assert!(Arc::ptr_eq(&originals[0].original, &control.control));
        let before = serde_json::to_value(&view).unwrap();
        drop(lease);
        assert!(control.control.released.get());
        assert!(!control.control.responses.get());
        assert_eq!(serde_json::to_value(&view).unwrap(), before);
        assert_eq!(before["routes"][0]["state"], "active");
    }
    drop(credit);
    assert_eq!(
        engine.packet_routes_snapshot(&Default::default()).routes[0].state,
        PacketRouteState::Closing
    );
    control.responses_stopped();
    assert_eq!(engine.packet_routes_snapshot(&Default::default()).total, 0);
    assert_eq!(book.usage().response_bytes, 0);
}
#[test]
fn rejected_workspace_never_clones_the_original_route_or_acquires_a_response_owner() {
    let engine = engine();
    let lease = route(&engine, "actual");
    let count = Arc::strong_count(&lease.control);
    let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
    limits.messages.response_bytes_total = 1;
    let book = ControlBudget::new(limits);
    let mut credit = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let request = QueryRequest::PacketRoutes(Default::default());
    {
        let guard = engine.control_runtime_query(&request).unwrap();
        assert_eq!(
            guard.freeze(&mut credit).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(Arc::strong_count(&lease.control), count);
        assert!(lease.control.responses.get());
        assert!(!lease.control.close.get());
    }
    drop(credit);
    assert_eq!(book.usage().response_bytes, 0);
}
