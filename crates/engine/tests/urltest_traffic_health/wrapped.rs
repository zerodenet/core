use super::*;

#[test]
fn selector_wrapped_urltest_skips_a_carrier_cooled_by_a_pinned_path() {
    let engine = engine(
        &["primary", "alternate"],
        vec![
            json!({"tag": "wrapper", "type": "selector", "outbounds": ["auto"]}),
            json!({"tag": "pinned", "type": "selector", "outbounds": ["primary"]}),
        ],
    );
    assert_eq!(single_index(resolve(&engine, "pinned")), 0);
    quarantine(&engine, "primary");

    let (resolved, _, selections) = engine
        .resolve_route_decision_for_flow(
            RouteDecision::Route("wrapper".to_owned()),
            &Address::Domain("application.example".to_owned()),
            443,
        )
        .expect("resolve wrapped URLTest");
    assert_eq!(single_index(resolved), 1);
    assert_eq!(selections.len(), 1);
    assert_eq!(selections[0].policy_tag, "auto");
    assert_eq!(selections[0].member_tag, "alternate");
    assert_eq!(single_index(resolve(&engine, "pinned")), 0);

    let snapshot = engine.runtime_snapshot();
    let plan = snapshot.plan();
    assert_eq!(
        snapshot.urltest_selected_target(plan.target_id("auto").unwrap()),
        plan.target_id("alternate")
    );
    engine.record_outbound_success("primary");
    assert_eq!(single_index(resolve(&engine, "wrapper")), 1);
    assert_eq!(single_index(resolve(&engine, "pinned")), 0);
}

#[test]
fn outer_fallback_retains_a_cooling_urltest_candidate_before_its_configured_backup() {
    let engine = engine(
        &["primary", "alternate"],
        vec![json!({"tag": "outer", "type": "fallback",
            "outbounds": ["auto", "direct"]})],
    );
    quarantine(&engine, "primary");
    quarantine(&engine, "alternate");

    let (resolved, _, selections) = engine
        .resolve_route_decision_for_flow(
            RouteDecision::Route("outer".to_owned()),
            &Address::Domain("application.example".to_owned()),
            443,
        )
        .expect("cooldown must not make URLTest unresolvable");
    let ResolvedOutbound::Fallback { candidates } = resolved else {
        panic!("expected configured fallback candidates");
    };
    assert_eq!(candidates.len(), 2);
    assert_eq!(
        single_index(ResolvedOutbound::Single(candidates[0].clone())),
        1
    );
    assert!(matches!(
        candidates[1],
        ResolvedLeafOutbound::Direct { tag: Some("direct") }
    ));
    assert_eq!(selections.len(), 1);
    assert_eq!(selections[0].member_tag, "alternate");
}
