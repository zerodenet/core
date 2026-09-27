use zero_api::{event_type, EventFilter};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_engine::{
    Engine, OutboundIdentity, PassiveRelayOutcome, ResolvedLeafOutbound, ResolvedOutbound,
    RouteDecision,
};

fn engine() -> Engine {
    let config: RuntimeConfig = serde_json::from_str(
        r#"
        {
          "inbounds": [{
            "tag": "entry",
            "listen": { "address": "127.0.0.1", "port": 10080 },
            "protocol": { "type": "direct", "target": "landing.example", "port": 14788 }
          }],
          "outbounds": [
            {
              "tag": "primary",
              "protocol": {
                "type": "shadowsocks",
                "server": "primary.example",
                "port": 443,
                "password": "password",
                "cipher": "aes-256-gcm"
              }
            },
            {
              "tag": "alternate",
              "protocol": {
                "type": "shadowsocks",
                "server": "alternate.example",
                "port": 443,
                "password": "password",
                "cipher": "aes-256-gcm"
              }
            },
            {
              "tag": "direct-out",
              "protocol": { "type": "direct" }
            }
          ],
          "outbound_groups": [{
            "tag": "auto",
            "type": "url_test",
            "outbounds": ["primary", "alternate"],
            "url": "http://probe.example/",
            "interval_seconds": 60
          }, {
            "tag": "wrapper",
            "type": "selector",
            "outbounds": ["auto"]
          }, {
            "tag": "pinned",
            "type": "selector",
            "outbounds": ["primary"]
          }, {
            "tag": "later",
            "type": "fallback",
            "outbounds": ["auto", "direct-out"]
          }],
          "mode": { "type": "global", "outbound": "auto" },
          "route": {
            "rules": [],
            "final": { "type": "route", "outbound": "auto" }
          }
        }
        "#,
    )
    .expect("parse config");
    Engine::new(config).expect("build engine")
}

fn selected_identity(engine: &Engine, target: &Address, port: u16) -> (OutboundIdentity, String) {
    let (resolved, _, selections) = engine
        .resolve_route_decision_for_flow(RouteDecision::Route("auto".to_owned()), target, port)
        .expect("resolve flow");
    let ResolvedOutbound::Single(ResolvedLeafOutbound::Proxy { identity }) = resolved else {
        panic!("expected one proxy leaf");
    };
    (identity, selections[0].member_tag.clone())
}

#[test]
fn early_failures_move_only_the_affected_target_to_an_alternate() {
    let engine = engine();
    let target = Address::Domain("landing.example".to_owned());
    let (primary_identity, primary_tag) = selected_identity(&engine, &target, 14788);
    assert_eq!(primary_identity.config_index(), 0);
    assert_eq!(primary_tag, "primary");

    let selection = zero_engine::PassiveRelaySelection {
        policy_tag: "auto".to_owned(),
        member_tag: "primary".to_owned(),
        half_open: false,
    };
    for _ in 0..3 {
        engine.record_passive_relay_outcome(
            &selection,
            &target,
            14788,
            PassiveRelayOutcome::Failure,
        );
    }

    let (alternate_identity, alternate_tag) = selected_identity(&engine, &target, 14788);
    assert_eq!(alternate_identity.config_index(), 1);
    assert_eq!(alternate_tag, "alternate");

    let (unaffected_identity, unaffected_tag) = selected_identity(&engine, &target, 14688);
    assert_eq!(unaffected_identity.config_index(), 0);
    assert_eq!(unaffected_tag, "primary");
}

#[test]
fn quarantine_transition_is_visible_in_control_plane_events() {
    let engine = engine();
    let target = Address::Domain("landing.example".to_owned());
    let selection = zero_engine::PassiveRelaySelection {
        policy_tag: "auto".to_owned(),
        member_tag: "primary".to_owned(),
        half_open: false,
    };

    for _ in 0..3 {
        engine.record_passive_relay_outcome(
            &selection,
            &target,
            14788,
            PassiveRelayOutcome::Failure,
        );
    }

    let events = engine.events_snapshot(&EventFilter {
        event_types: vec![event_type::POLICY_PASSIVE_RELAY_HEALTH_CHANGED.to_owned()],
        ..EventFilter::default()
    });
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload["state"], "quarantined");
    assert_eq!(events[0].payload["policy_tag"], "auto");
    assert_eq!(events[0].payload["member_tag"], "primary");
    assert_eq!(events[0].payload["port"], 14788);
    assert_eq!(events[0].payload["quarantine_duration_ms"], 15_000);
}

#[test]
fn selector_wrapped_urltest_skips_a_leaf_quarantined_by_another_policy_path() {
    let engine = engine();
    let target = Address::Domain("landing.example".to_owned());
    let (pinned, _, _) = engine
        .resolve_route_decision_for_flow(RouteDecision::Route("pinned".to_owned()), &target, 443)
        .expect("resolve explicit selector");
    assert!(matches!(
        pinned,
        ResolvedOutbound::Single(ResolvedLeafOutbound::Proxy { identity })
            if identity.config_index() == 0
    ));
    for _ in 0..5 {
        engine.record_outbound_failure("primary");
    }
    let (resolved, _, selections) = engine
        .resolve_route_decision_for_flow(RouteDecision::Route("wrapper".to_owned()), &target, 443)
        .expect("resolve wrapped urltest");
    assert!(matches!(
        resolved,
        ResolvedOutbound::Single(ResolvedLeafOutbound::Proxy { identity })
            if identity.config_index() == 1
    ));
    assert_eq!(selections[0].member_tag, "alternate");

    let (pinned, _, _) = engine
        .resolve_route_decision_for_flow(RouteDecision::Route("pinned".to_owned()), &target, 443)
        .expect("explicit selector remains pinned");
    assert!(matches!(
        pinned,
        ResolvedOutbound::Single(ResolvedLeafOutbound::Proxy { identity })
            if identity.config_index() == 0
    ));
}

#[test]
fn urltest_reports_no_usable_member_and_outer_fallback_keeps_its_next_candidate() {
    let engine = engine();
    let target = Address::Domain("landing.example".to_owned());
    for tag in ["primary", "alternate"] {
        for _ in 0..5 {
            engine.record_outbound_failure(tag);
        }
    }
    let error = engine
        .resolve_route_decision_for_flow(RouteDecision::Route("auto".to_owned()), &target, 443)
        .expect_err("all urltest leaves are quarantined");
    assert_eq!(error.code(), "no_usable_urltest_member");
    assert!(error.to_string().contains("auto"));

    let (resolved, _, selections) = engine
        .resolve_route_decision_for_flow(RouteDecision::Route("later".to_owned()), &target, 443)
        .expect("outer fallback still has direct candidate");
    assert!(selections.is_empty());
    assert!(matches!(
        resolved,
        ResolvedOutbound::Fallback { candidates }
            if matches!(candidates.as_slice(), [ResolvedLeafOutbound::Direct { .. }])
    ));
}
