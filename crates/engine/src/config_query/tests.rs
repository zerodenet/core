use super::*;
use crate::{plan::UrlTestSelectionReason, Engine, UrlTestMemberState, UrlTestSelection};
use zero_api::QueryService;
fn engine() -> Engine {
    let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[
            {"tag":"primary","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}},
            {"tag":"alternate-雪","protocol":{"type":"socks5","server":"127.0.0.1","port":1081}}
        ],
        "outbound_groups":[
            {"tag":"pick","type":"selector","outbounds":["primary","alternate-雪"]},
            {"tag":"fallback","type":"fallback","outbounds":["pick","alternate-雪"]},
            {"tag":"chain","type":"relay","proxies":["pick","alternate-雪"]},
            {"tag":"balanced","type":"load_balance","strategy":"round_robin","outbounds":["fallback","chain"]},
            {"tag":"automatic","type":"url_test","outbounds":["primary","alternate-雪"],"url":"http://example.invalid/probe","interval_seconds":30}
        ],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    Engine::new(config).unwrap()
}
fn compare(engine: &Engine, request: QueryRequest) {
    let expected = engine.query(request.clone()).unwrap();
    let expected = serde_json::to_value(expected).unwrap();
    let snapshot = engine.runtime_snapshot();
    let query = snapshot.control_config_query(&request).unwrap().unwrap();
    assert_eq!(serde_json::to_value(&query).unwrap(), expected);
    let expected_body = expected.as_object().unwrap().values().next().unwrap();
    assert_eq!(&serde_json::to_value(query.body()).unwrap(), expected_body);
}
#[test]
fn actual_borrowed_config_and_all_policy_kinds_keep_the_original_wire_values() {
    let engine = engine();
    for request in [
        QueryRequest::Config(Default::default()),
        QueryRequest::Policies(Default::default()),
    ] {
        compare(&engine, request);
    }
    for tag in ["pick", "fallback", "chain", "balanced", "automatic"] {
        compare(
            &engine,
            QueryRequest::Policy(zero_api::PolicyGetQuery {
                policy_tag: tag.into(),
            }),
        );
    }
    engine.set_selector_target("pick", "alternate-雪").unwrap();
    compare(&engine, QueryRequest::Config(Default::default()));
}
#[test]
fn original_urltest_history_and_escaped_errors_are_borrowed_without_changing_fields() {
    let engine = engine();
    let snapshot = engine.runtime_snapshot();
    let id = snapshot.plan().target_id("automatic").unwrap();
    let a = snapshot.plan().target_id("primary").unwrap();
    let b = snapshot.plan().target_id("alternate-雪").unwrap();
    snapshot.update_urltest_state(
        id,
        b,
        Some(17),
        vec![UrlTestMemberState {
            member_id: a,
            healthy: false,
            latency_ms: None,
            last_checked_unix_ms: Some(42),
            last_error: Some("real \"history\"\\\n雪".into()),
            effective_chains: vec![vec![id, a]],
        }],
        UrlTestSelection {
            previous: Some(a),
            selected: b,
            best: Some(b),
            current_latency_ms: Some(32),
            best_latency_ms: Some(17),
            tolerance_ms: 3,
            switched: true,
            reason: UrlTestSelectionReason::BetterBeyondTolerance,
        },
    );
    compare(&engine, QueryRequest::Config(Default::default()));
    compare(
        &engine,
        QueryRequest::Policy(zero_api::PolicyGetQuery {
            policy_tag: "automatic".into(),
        }),
    );
    let request = QueryRequest::Policy(zero_api::PolicyGetQuery {
        policy_tag: "not-present\"雪".into(),
    });
    let missing = snapshot
        .control_config_query(&request)
        .unwrap()
        .unwrap_err();
    assert_eq!(
        missing.message_len().unwrap(),
        missing.message().to_string().len()
    );
    assert_eq!(missing.into_api_error(), engine.query(request).unwrap_err());
}
#[test]
fn same_view_keeps_its_actual_snapshot_after_a_new_configuration_is_installed() {
    let engine = engine();
    let request = QueryRequest::Config(Default::default());
    let snapshot = engine.runtime_snapshot();
    let expected = serde_json::to_vec(&engine.query(request.clone()).unwrap()).unwrap();
    // Hold the same immutable generation. Replacing config does not mutate its
    // strings/plan; encoding this original view cannot re-query the new Engine.
    let next =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    engine.reload_config(next).unwrap();
    let query = snapshot.control_config_query(&request).unwrap().unwrap();
    assert_eq!(serde_json::to_vec(query).unwrap(), expected);
    assert_ne!(
        serde_json::to_vec(&engine.query(request).unwrap()).unwrap(),
        expected
    );
}
