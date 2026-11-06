use super::*;
use zero_core::{
    component::{ComponentId, EndpointId, ResourceGeneration},
    packet::{IpPacket, ReturnAssociation},
};
fn frame(bytes: Vec<u8>, cookie: u64) -> IpPacket {
    IpPacket::new(
        bytes.into(),
        ReturnAssociation {
            endpoint: EndpointId(ComponentId(41).0),
            generation: ResourceGeneration(9),
            peer: Some(zero_core::component::PeerId(
                "actual-authenticated-source".into(),
            )),
            correlation: cookie,
        },
    )
    .unwrap()
}
fn engine() -> zero_engine::Engine {
    zero_engine::Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
#[tokio::test]
async fn standard_route_retains_original_cookie_and_closes_inline_without_a_response_pump() {
    let engine = engine();
    let mut pins = PacketSessionPins::default().managed(engine.clone(), "actual-source".into());
    let request = frame(
        zero_stack::packet::build_udp(
            "10.0.0.2".parse().unwrap(),
            "198.51.100.10".parse().unwrap(),
            40000,
            443,
            b"original",
        ),
        67,
    );
    let association = request.return_association.clone();
    let source_peer = association.peer.as_ref().map(|peer| peer.0.clone());
    let plane = PacketPlane::Packet("actual-outbound".into());
    assert!(pins.admit_standard(&request, &plane, source_peer.clone()));
    assert!(pins.record_observed_key(
        zero_stack::packet::packet_conversation_key(request.payload()),
        plane,
        source_peer,
        Some("selected-peer".into()),
        None
    ));
    let response = frame(
        zero_stack::packet::build_udp(
            "198.51.100.10".parse().unwrap(),
            "10.0.0.2".parse().unwrap(),
            443,
            40000,
            b"original",
        ),
        67,
    );
    assert!(pins.permits_standard_return(&response));
    let wrong = frame(response.payload().to_vec(), 68);
    assert!(!pins.permits_standard_return(&wrong));
    assert_eq!(response.return_association, association);
    let active = engine.packet_routes_snapshot(&zero_api::PacketRouteListQuery::default());
    assert_eq!(active.total, 1);
    let (_, control) = engine
        .begin_close_packet_route(&zero_api::PacketRouteCloseCommand {
            route_id: active.routes[0].route_id.clone(),
            expected_core_instance_id: engine.core_instance_id().into(),
            expected_config_revision: None,
        })
        .unwrap();
    assert!(!pins.permits_standard_return(&response));
    tokio::time::timeout(std::time::Duration::from_secs(1), pins.management_changed())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), control.wait_released())
        .await
        .unwrap();
    assert_eq!(
        engine
            .packet_routes_snapshot(&zero_api::PacketRouteListQuery::default())
            .total,
        0
    );
    assert_eq!(response.return_association, association);
}
