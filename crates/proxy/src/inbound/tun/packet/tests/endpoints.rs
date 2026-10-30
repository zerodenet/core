use super::*;
use crate::inventory::PacketRouteTarget;
use std::sync::Arc;
use zero_api::{EndpointDirections, EndpointGetQuery};
use zero_engine::EndpointChange;

#[tokio::test]
async fn warmed_packet_route_observes_endpoint_overrides_without_config_revision_change() {
    let fixture = Fixture::new(json!({
        "endpoints":[{
            "tag":"wg","directions":{"inbound":true,"outbound":true},
            "listen":{"address":"127.0.0.1","port":18889},
            "protocol":{"type":"wireguard","private_key":"01".repeat(32),
                "addresses":["10.222.0.2/32"],"peers":[{
                    "public_key":"02".repeat(32),"endpoint":"127.0.0.1:51820",
                    "allowed_ips":["0.0.0.0/0","::/0"]
                }]}
        }],
        "route":{"rules":[],"final":{"type":"route","outbound":"wg"},"final_mode":"packet"}
    }));
    let destination = "203.0.113.1".parse().unwrap();
    let target = || {
        fixture
            .route
            .packet_route_target(destination, Some(packet::IPPROTO_UDP))
    };
    let engine = fixture.proxy.engine();
    let original = engine.runtime_snapshot();
    let revision = engine.config_revision();
    let query = EndpointGetQuery {
        endpoint_id: "endpoint:wg".into(),
    };
    let intent_revision = engine.endpoint_snapshot(&query).unwrap().intent_revision;
    for _ in 0..3 {
        assert!(matches!(target(), PacketRouteTarget::Packet { .. }));
    }
    for change in [
        EndpointChange::Enabled(false),
        EndpointChange::Directions(EndpointDirections {
            inbound: true,
            outbound: false,
        }),
    ] {
        // Use the same engine-owned preparation and staging boundaries as
        // endpoint control, without starting any WireGuard socket or listener.
        let changed = engine
            .prepare_endpoint_change(
                &original,
                &query.endpoint_id,
                change,
                false,
                Some(intent_revision),
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&original, &changed));
        assert!(Arc::ptr_eq(original.config(), changed.config()));
        assert_eq!(changed.config_revision(), revision);
        engine.restore_staged_snapshot(changed, false).unwrap();
        assert_eq!(engine.config_revision(), revision);
        assert!(engine.endpoint_snapshot(&query).unwrap().intent_revision > intent_revision);
        assert!(matches!(target(), PacketRouteTarget::Unsupported));

        engine
            .restore_staged_snapshot(original.clone(), false)
            .unwrap();
        assert!(matches!(target(), PacketRouteTarget::Packet { .. }));
    }
}
