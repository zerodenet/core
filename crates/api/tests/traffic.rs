use zero_api::*;
#[test]
fn traffic_queries_reset_and_null_metrics_have_stable_wire_contracts() {
    let one = QueryRequest::TrafficStat(TrafficGetQuery {
        scope: TrafficScope::Peer {
            endpoint_id: "endpoint:a".into(),
            peer_id: "stable-peer".into(),
        },
    });
    let encoded = serde_json::to_value(&one).unwrap();
    assert_eq!(encoded["traffic_stat"]["scope"]["kind"], "peer");
    assert_eq!(
        serde_json::from_value::<QueryRequest>(encoded).unwrap(),
        one
    );
    let page = QueryRequest::TrafficStats(TrafficListQuery {
        expected_registry_revision: Some(2),
        ..Default::default()
    });
    assert_eq!(
        serde_json::from_value::<QueryRequest>(serde_json::to_value(&page).unwrap()).unwrap(),
        page
    );
    let reset = CommandRequest::StatsReset(StatsResetCommand {
        expected_core_instance_id: "core-a".into(),
        operation_id: None,
        targets: vec![StatsResetTarget {
            scope: TrafficScope::Global,
            expected_stats_epoch: "epoch-a".into(),
            expected_generation: None,
        }],
    });
    assert_eq!(reset.required_permission(), Permission::Admin);
    let encoded = serde_json::to_value(&reset).unwrap();
    assert_eq!(encoded["method"], "stats.reset");
    assert_eq!(
        serde_json::from_value::<CommandRequest>(encoded).unwrap(),
        reset
    );
    let counters = serde_json::to_value(TrafficCounters {
        rx_bytes: Some(0),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(counters["rx_bytes"], 0);
    assert!(counters["rx_packets"].is_null());
    assert!(event_type::is_known(event_type::STATS_RESET));
    assert!(event_type::is_known(event_type::STATS_SCOPES_SAMPLED));
    assert!(serde_json::from_value::<StatsResetCommand>(
        serde_json::json!({"expected_core_instance_id":"x","targets":[],"metrics":["rx_bytes"]})
    )
    .is_err());
}

#[test]
fn host_scope_and_additive_discard_fields_preserve_legacy_deserialization() {
    let host = QueryRequest::TrafficStat(TrafficGetQuery {
        scope: TrafficScope::HostInterface { name: "en0".into() },
    });
    let value = serde_json::to_value(&host).unwrap();
    assert_eq!(value["traffic_stat"]["scope"]["kind"], "host_interface");
    assert_eq!(serde_json::from_value::<QueryRequest>(value).unwrap(), host);
    let counters: TrafficCounters = serde_json::from_value(serde_json::json!({
        "bytes_up":null,"bytes_down":null,"rx_bytes":0,"tx_bytes":null,
        "rx_packets":null,"tx_packets":null,"dropped_packets":null,"errors":null
    }))
    .unwrap();
    assert_eq!(counters.rx_dropped_packets, None);
    let plane: TrafficPlaneSnapshot = serde_json::from_value(serde_json::json!({
        "plane":"inner","accounting_basis":"legacy","counters":counters,
        "available_metrics":["rx_bytes"],"resettable_metrics":["rx_bytes"]
    }))
    .unwrap();
    assert!(plane.drop_reasons.is_empty());
    assert_eq!(plane.drop_coverage, None);
    let query: TrafficListQuery = serde_json::from_value(serde_json::json!({})).unwrap();
    assert!(!query.include_host_interfaces);
    assert!(serde_json::to_value(&query)
        .unwrap()
        .get("include_host_interfaces")
        .is_none());
    let opted_in = TrafficListQuery {
        include_host_interfaces: true,
        ..query
    };
    let wire = serde_json::to_value(&opted_in).unwrap();
    assert_eq!(wire["include_host_interfaces"], true);
    assert_eq!(
        serde_json::from_value::<TrafficListQuery>(wire).unwrap(),
        opted_in
    );
    assert!(event_type::is_known(
        event_type::STATS_HOST_INTERFACES_SAMPLED
    ));
}
