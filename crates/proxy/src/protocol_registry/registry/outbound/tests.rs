use std::io;
use std::sync::Arc;

use super::{ClaimedOutboundLeaf, ClaimedTcpHooks, ClaimedUdpHooks};
use crate::protocol_registry::{ClaimedPacketLeaf, OutboundLeafRuntime};
use crate::runtime::network_graph::{NetworkGraph, Plane};
use crate::runtime::packet_route::{PacketForwardObservation, PreparedPacketRouteOperation};
use crate::runtime::path::TcpPathCategory;

struct PacketOnlyLeaf;

impl ClaimedPacketLeaf for PacketOnlyLeaf {
    fn prepare_packet_route(&self) -> Box<dyn PreparedPacketRouteOperation> {
        Box::new(PacketOnlyOperation)
    }
}

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

#[test]
fn packet_only_leaf_requires_no_tcp_or_udp_capability() {
    let leaf = ClaimedOutboundLeaf::new(
        OutboundLeafRuntime::virtual_outbound(
            "packet-only",
            "packet-only",
            TcpPathCategory::Unavailable,
        ),
        ClaimedTcpHooks::default(),
        ClaimedUdpHooks::default(),
        Some(Arc::new(PacketOnlyLeaf)),
    );
    assert!(leaf.prepare_tcp_connect(None).is_err());
    assert!(leaf.prepare_udp_flow(None).is_err());
    let path = NetworkGraph::packet_ingress()
        .shortest_path(Plane::Packet, leaf.data_plane_sinks(), None)
        .expect("packet-only outbound remains routable");
    assert_eq!(path.planes, [Plane::Packet]);
}
