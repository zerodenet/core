use super::*;
use zero_api::TrafficGetQuery;

#[test]
fn outer_observer_requires_an_attached_carrier_and_degrades_incomplete_packet_counts() {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap();
    let engine = Engine::new(config).unwrap();
    let scope = TrafficScope::Outbound {
        tag: "direct".into(),
    };
    let query = || {
        engine
            .traffic_snapshot(&TrafficGetQuery {
                scope: scope.clone(),
            })
            .unwrap()
    };
    let observer = outbound(&engine, "direct", TrafficPlane::Outer).unwrap();
    assert_eq!(query().planes[2].counters.rx_bytes, None);
    assert_eq!(query().planes[2].counters.errors, None);
    observer.datagram_boundary();
    assert_eq!(query().planes[2].counters.rx_bytes, Some(0));
    observer.received_datagram(5);
    assert_eq!(query().planes[2].counters.rx_bytes, Some(5));
    assert_eq!(query().planes[2].counters.rx_packets, Some(1));
    observer.stream_boundary();
    observer.datagram_boundary();
    assert_eq!(query().planes[2].counters.rx_packets, None);
    assert_eq!(query().planes[2].counters.rx_bytes, Some(5));
    observer.error();
    assert_eq!(query().planes[2].counters.errors, Some(1));
}
#[test]
fn bounded_rx_attribution_loss_does_not_hide_measured_tx() {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap();
    let engine = Engine::new(config).unwrap();
    let observer = outbound(&engine, "direct", TrafficPlane::Inner).unwrap();
    observer.sent(32);
    observer.received(32);
    observer.receive_coverage_lost();
    let snapshot = engine
        .traffic_snapshot(&TrafficGetQuery {
            scope: TrafficScope::Outbound {
                tag: "direct".into(),
            },
        })
        .unwrap();
    assert_eq!(snapshot.planes[1].counters.rx_bytes, None);
    assert_eq!(snapshot.planes[1].counters.rx_packets, None);
    assert_eq!(snapshot.planes[1].counters.tx_bytes, Some(32));
    assert_eq!(snapshot.planes[1].counters.tx_packets, Some(1));
}
