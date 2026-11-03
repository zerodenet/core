use std::io;
use std::sync::Arc;

use super::ClaimedOutboundLeaf;
use crate::protocol_registry::OutboundLeafRuntime;
use crate::runtime::network_graph::{NetworkGraph, Plane};
use crate::runtime::packet_route::{PacketForwardObservation, PreparedPacketRouteOperation};
use crate::runtime::path::TcpPathCategory;

struct PacketOnlyOperation;

#[async_trait::async_trait]
impl PreparedPacketRouteOperation for PacketOnlyOperation {
    async fn forward(
        &self,
        _packet: &mut zero_traits::PacketBuffer,
        _ingress_id: u64,
        _replies: zero_stack::packet_output::PacketSender,
        _egress_generation: u64,
        _observer: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<PacketForwardObservation> {
        Ok(PacketForwardObservation::forwarded(None))
    }
}

fn packet_leaf(
    flow: Option<crate::runtime::packet_flow::PacketFlowAdapter>,
    tcp: Option<crate::protocol_registry::TcpConnectResult>,
) -> ClaimedOutboundLeaf<'static> {
    let tcp_path = if flow.is_some() {
        TcpPathCategory::Session
    } else {
        TcpPathCategory::Unavailable
    };
    ClaimedOutboundLeaf::new(
        OutboundLeafRuntime::virtual_outbound("packet-only", "packet-only", tcp_path),
        crate::protocol_registry::OutboundLeafClaim {
            tcp_path,
            tcp,
            tcp_relay: None,
            udp: None,
            udp_relay: None,
            packet_path: None,
            packet: Some(crate::runtime::packet_route::PacketRouteOperations {
                native: Arc::new(PacketOnlyOperation),
                translated: None,
                datagram: None,
                flow,
                correlated_returns: false,
            }),
        },
    )
}

#[test]
fn packet_only_leaf_requires_no_tcp_or_udp_capability() {
    let leaf = packet_leaf(None, None);
    assert!(leaf.tcp_connect().is_err());
    let path = NetworkGraph::packet_ingress()
        .shortest_path(Plane::Packet, leaf.data_plane_sinks(), None)
        .expect("packet-only outbound remains routable");
    assert_eq!(path.planes, [Plane::Packet]);
    assert!(leaf.into_udp_flow().is_err());
    for mode in [
        zero_engine::RouteMode::Auto,
        zero_engine::RouteMode::Packet,
        zero_engine::RouteMode::Flow,
    ] {
        let leaf = packet_leaf(None, None);
        assert!(leaf.tcp_connect_for_route(mode).is_err());
        assert!(leaf.into_udp_flow_for_route(mode).is_err());
    }
}

#[tokio::test]
async fn packet_only_leaf_uses_opt_in_converter_without_native_l4_hooks() {
    let fixture = crate::runtime::packet_flow::tests::support::Fixture::new(1420);
    for mode in [
        zero_engine::RouteMode::Auto,
        zero_engine::RouteMode::Packet,
        zero_engine::RouteMode::Flow,
        zero_engine::RouteMode::Translate,
    ] {
        let leaf = packet_leaf(Some(fixture.adapter.clone()), None);
        assert!(!leaf.has_tcp_capability());
        assert!(!leaf.has_udp_flow_capability());
        assert!(leaf.tcp_connect_for_route(mode).is_ok());
        assert!(leaf.into_udp_flow_for_route(mode).is_ok());
    }
}

#[tokio::test]
async fn auto_keeps_the_materialized_stream_and_packet_mode_selects_only_the_converter() {
    let fixture = crate::runtime::packet_flow::tests::support::Fixture::new(1420);
    let actual: Arc<dyn crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation> =
        fixture.adapter.prepare_tcp().into();
    let leaf = packet_leaf(Some(fixture.adapter.clone()), Some(Ok(actual.clone())));
    let auto = leaf
        .tcp_connect_for_route(zero_engine::RouteMode::Auto)
        .unwrap_or_else(|_| panic!("actual Stream"));
    assert!(Arc::ptr_eq(&auto, &actual));
    let packet = leaf
        .tcp_connect_for_route(zero_engine::RouteMode::Packet)
        .unwrap_or_else(|_| panic!("actual converter"));
    assert!(!Arc::ptr_eq(&packet, &actual));
    let rejected = packet_leaf(
        Some(fixture.adapter.clone()),
        Some(Err(("invalid_tcp", "dormant TCP error".into()))),
    );
    assert_eq!(
        rejected
            .tcp_connect_for_route(zero_engine::RouteMode::Auto)
            .err()
            .unwrap()
            .stage,
        "invalid_tcp"
    );
    assert!(rejected
        .tcp_connect_for_route(zero_engine::RouteMode::Packet)
        .is_ok());
}

#[tokio::test]
async fn dormant_udp_failure_does_not_poison_stream_or_strict_packet_selection() {
    let fixture = crate::runtime::packet_flow::tests::support::Fixture::new(1420);
    for mode in [zero_engine::RouteMode::Auto, zero_engine::RouteMode::Packet] {
        let mut leaf = packet_leaf(
            Some(fixture.adapter.clone()),
            Some(Ok(fixture.adapter.prepare_tcp().into())),
        );
        leaf.udp = Some(Err(crate::runtime::udp_dispatch::FlowFailure {
            stage: "cold_udp",
            error: io::Error::from(io::ErrorKind::InvalidInput).into(),
            upstream: Some(("node.test".into(), 443)),
        }));
        assert!(leaf.tcp_connect_for_route(mode).is_ok());
        let udp = leaf.into_udp_flow_for_route(mode);
        if mode == zero_engine::RouteMode::Packet {
            assert!(udp.is_ok());
        } else {
            let error = udp.err().unwrap();
            assert_eq!(error.stage, "cold_udp");
            assert_eq!(error.upstream, Some(("node.test".into(), 443)));
            assert!(
                matches!(error.error, zero_engine::EngineError::Io(error) if error.kind()==io::ErrorKind::InvalidInput)
            );
        }
    }
}

#[test]
fn packet_projections_reuse_the_actual_materialized_executor() {
    let leaf = packet_leaf(None, None);
    let first = leaf.prepare_packet_route().unwrap();
    let second = leaf.prepare_packet_route().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(leaf.prepare_translated_packet_route().is_none());
    assert!(leaf.prepare_datagram_exchange().is_none());
    assert!(!leaf.native_packet_returns_correlated());
}
