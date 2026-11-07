use super::*;
use zero_api::{QueryRequest, QueryService};
fn handle() -> ProxyHandle {
    let config=zero_config::RuntimeConfig::parse(r#"{
        "outbounds":[{"tag":"a","protocol":{"type":"direct"}},{"tag":"longer-雪","protocol":{"type":"direct"}}],
        "outbound_groups":[{"tag":"pick","type":"selector","outbounds":["a","longer-雪"]}],
        "route":{"final":{"type":"direct"}}
    }"#).unwrap();
    let proxy = zero_proxy::Proxy::new(config).unwrap();
    ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy,
    )
}
#[test]
fn same_query_snapshot_is_encoded_once_and_its_original_buffer_holds_the_root_credit() {
    let handle = handle();
    let budget = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    let request = QueryRequest::Config(Default::default());
    let id = Some(serde_json::json!({"opaque":[17,"same-雪"]}));
    let expected = serde_json::to_value(ApiResponse::ok_with_id(
        id.clone(),
        handle.query(request.clone()).unwrap(),
    ))
    .unwrap();
    let original = prepare(&handle, &request, id, &budget).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(original.bytes()).unwrap(),
        expected
    );
    assert_eq!(budget.usage().response_bytes, original.bytes().len());
    let mut target = budget.limits();
    target.messages.response_bytes_total = original.bytes().len() - 1;
    match budget.prepare(target) {
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
        Ok(_) => panic!("held original encoded buffer was not charged"),
    }
    // Holding encoded bytes does not hold an Engine guard across IO.
    handle
        .engine_handle()
        .inner()
        .set_selector_target("pick", "longer-雪")
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(original.bytes()).unwrap(),
        expected
    );
    drop(original);
    assert_eq!(budget.usage().response_bytes, 0);
    let next = prepare(&handle, &request, None, &budget).unwrap();
    assert!(std::str::from_utf8(next.bytes())
        .unwrap()
        .contains("longer-雪"));
}
#[test]
fn capacity_rejection_and_original_not_found_errors_leave_no_engine_guard_or_bytes_behind() {
    let handle = handle();
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.response_bytes_total = 1;
    let budget = ControlBudget::new(limits);
    let request = QueryRequest::Config(Default::default());
    let error = prepare(&handle, &request, None, &budget).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(input::is_invalid_input_frame(&error));
    let capacity = error
        .get_ref()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(capacity.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(budget.usage().response_bytes, 0);
    handle
        .engine_handle()
        .inner()
        .set_selector_target("pick", "longer-雪")
        .unwrap();
    let budget = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    let request = QueryRequest::Policy(zero_api::PolicyGetQuery {
        policy_tag: "missing-\"雪".into(),
    });
    let error = handle.query(request.clone()).unwrap_err();
    let expected = crate::ipc::owner::BorrowedErrorResponse::new(None, &error);
    assert!(crate::ipc::owner::serialized_len(&expected).is_ok());
    let original = prepare(&handle, &request, None, &budget).unwrap();
    let value: serde_json::Value = serde_json::from_slice(original.bytes()).unwrap();
    assert_eq!(value["ok"], false);
    assert_eq!(value["error"]["code"], "not_found");
    assert_eq!(
        value["error"]["message"],
        "policy `missing-\"雪` was not found"
    );
    drop(original);
    assert_eq!(budget.usage().response_bytes, 0);
}

#[test]
fn actual_runtime_and_stats_are_encoded_before_guards_leave_and_only_output_bytes_remain() {
    let handle = handle();
    let budget = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    for request in [
        QueryRequest::Stats(Default::default()),
        QueryRequest::PrincipalFlows(Default::default()),
        QueryRequest::Sinks(Default::default()),
        QueryRequest::ActiveFlows(zero_api::FlowListQuery {
            limit: None,
            filter: Default::default(),
        }),
        QueryRequest::RecentFlows(zero_api::FlowListQuery {
            limit: None,
            filter: Default::default(),
        }),
    ] {
        let expected =
            serde_json::to_value(ApiResponse::ok(handle.query(request.clone()).unwrap())).unwrap();
        let original = prepare(&handle, &request, None, &budget).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(original.bytes()).unwrap(),
            expected
        );
        // Original workspace/Engine guards are gone; only the actual wire Vec stays charged.
        assert_eq!(budget.usage().response_bytes, original.bytes().len());
        handle
            .engine_handle()
            .inner()
            .record_udp_upstream_association_created();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(original.bytes()).unwrap(),
            expected
        );
        drop(original);
        assert_eq!(budget.usage().response_bytes, 0);
    }
    let request = QueryRequest::Flow(zero_api::FlowGetQuery {
        flow_id: "missing-\"雪".into(),
    });
    let expected = serde_json::to_value(ApiResponse::<()>::from_api_error(
        &handle.query(request.clone()).unwrap_err(),
    ))
    .unwrap();
    let original = prepare(&handle, &request, None, &budget).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(original.bytes()).unwrap(),
        expected
    );
}

#[path = "tests/traffic.rs"]
mod traffic;

#[path = "tests/proxy.rs"]
mod proxy;
