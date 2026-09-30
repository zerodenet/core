use serde_json::json;
use zero_api::{
    event_type, CommandRequest, CommandService, EndpointGetQuery, EndpointListQuery,
    EndpointOperationCommand, EventFilter, QueryRequest, QueryResponse, QueryService,
};
use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{Engine, ResolvedLeafOutbound, ResolvedOutbound};

fn engine(enabled: bool) -> Engine {
    let config = json!({"endpoints":[{"tag":"wg", "enabled":enabled, "protocol":{
        "type":"wireguard", "private_key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=", "addresses":["10.0.0.1/32"],
        "peers":[{"public_key":"AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=", "endpoint":"127.0.0.1:51820", "allowed_ips":["10.0.0.0/24"]}]}}],
        "outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],
        "outbound_groups":[{"tag":"fixed", "type":"selector", "outbounds":["wg","direct"]},
            {"tag":"fallback", "type":"fallback", "outbounds":["wg","direct"]}],
        "route":{"rules":[], "final":{"type":"route", "outbound":"wg"}}});
    Engine::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap()
}

#[test]
fn endpoint_catalog_is_paginated_and_does_not_claim_device_execution() {
    let engine = engine(true);
    let QueryResponse::Endpoints(list) = engine
        .query(QueryRequest::Endpoints(EndpointListQuery {
            offset: 0,
            limit: Some(1),
        }))
        .unwrap()
    else {
        panic!("wrong response");
    };
    assert_eq!(list.total, 1);
    assert_eq!(list.endpoints[0].endpoint_id, "endpoint:wg");
    assert_eq!(
        list.endpoints[0].state,
        zero_api::EndpointRuntimeState::Stopped
    );
    assert!(!list.endpoints[0].effective.outbound);
    assert_eq!(
        engine
            .endpoint_snapshot(&EndpointGetQuery {
                endpoint_id: "missing".into()
            })
            .unwrap_err()
            .code,
        zero_api::ApiErrorCode::NotFound
    );
    assert!(engine
        .endpoints_snapshot(&EndpointListQuery {
            offset: usize::MAX,
            limit: Some(1)
        })
        .endpoints
        .is_empty());
    assert_eq!(
        engine
            .execute(CommandRequest::EndpointRestart(EndpointOperationCommand {
                endpoint_id: "endpoint:wg".into(),
                expected_intent_revision: None
            }))
            .unwrap_err()
            .code,
        zero_api::ApiErrorCode::Unsupported
    );
}

#[test]
fn disabled_endpoint_fails_fixed_routes_and_only_explicit_fallback_can_continue() {
    let engine = engine(false);
    let config = engine.config();
    let admission = zero_engine::EndpointAdmission::from_config(&config);
    assert!(!admission.device_enabled("wg"));
    assert_eq!(
        admission.outbound_denial("wg").unwrap().0,
        "endpoint_disabled"
    );
    for tag in ["wg", "fixed"] {
        let error = engine
            .resolve_route_decision(zero_engine::RouteDecision::Route(tag.into()))
            .unwrap_err();
        assert!(error.to_string().contains("endpoint_disabled"));
    }
    let (resolved, _plan) = engine
        .resolve_route_decision(zero_engine::RouteDecision::Route("fallback".into()))
        .unwrap();
    let ResolvedOutbound::Fallback { candidates } = resolved else {
        panic!("wrong route");
    };
    assert!(matches!(
        candidates.as_slice(),
        [ResolvedLeafOutbound::Direct { .. }]
    ));
}

#[test]
fn endpoint_counts_only_owned_active_flows_once_and_keeps_packet_counters_unavailable() {
    let engine = engine(true);
    let mut handles = Vec::new();
    for (network, outbound) in [
        (Network::Tcp, "wg"),
        (Network::Udp, "wg"),
        (Network::Tcp, "direct"),
    ] {
        let mut session = Session::new(
            0,
            Address::Domain("example.com".into()),
            443,
            network,
            ProtocolType::new("test"),
        );
        engine.prepare_session(&mut session, "socks-in").unwrap();
        session.outbound_tag = Some(outbound.into());
        engine.set_session_outbound(&session);
        handles.push(engine.track_session(session.id));
    }
    let endpoint = engine
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        })
        .unwrap();
    assert_eq!(endpoint.counters.active_stream_flows, Some(1));
    assert_eq!(endpoint.counters.active_datagram_flows, Some(1));
    assert_eq!(endpoint.counters.active_packet_routes, None);
    assert_eq!(endpoint.counters.inner_rx_bytes, None);
    drop(handles);
    let endpoint = engine
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        })
        .unwrap();
    assert_eq!(endpoint.counters.active_stream_flows, Some(0));
    assert_eq!(endpoint.counters.active_datagram_flows, Some(0));
}

#[test]
fn endpoint_stats_event_batches_available_counters_without_inventing_packet_data() {
    let engine = engine(true);
    let mut session = Session::new(
        0,
        Address::Domain("example.com".into()),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    engine.prepare_session(&mut session, "socks-in").unwrap();
    session.outbound_tag = Some("wg".into());
    engine.set_session_outbound(&session);
    let _flow = engine.track_session(session.id);

    engine.push_endpoint_stats_sampled();
    let events = engine.events_snapshot(&EventFilter {
        event_types: vec![event_type::ENDPOINT_STATS_SAMPLED.into()],
        ..EventFilter::default()
    });
    assert_eq!(events.len(), 1);
    let sample = &events[0].payload["samples"][0];
    assert_eq!(sample["endpoint_id"], "endpoint:wg");
    assert_eq!(sample["counters"]["active_stream_flows"], 1);
    assert!(sample["counters"]["active_packet_routes"].is_null());
    assert!(sample["counters"]["inner_rx_bytes"].is_null());
    assert!(!events[0].payload.to_string().contains("private_key"));
}
