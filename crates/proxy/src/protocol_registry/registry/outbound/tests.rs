use std::io;
use std::sync::Arc;

use super::{ClaimedOutboundLeaf, ClaimedTcpHooks, ClaimedUdpHooks};
use crate::protocol_registry::{ClaimedPacketLeaf, OutboundLeafRuntime};
use crate::runtime::network_graph::{NetworkGraph, Plane};
use crate::runtime::packet_route::{PacketForwardObservation, PreparedPacketRouteOperation};
use crate::runtime::path::TcpPathCategory;

struct PacketOnlyLeaf(Option<crate::runtime::packet_flow::PacketFlowAdapter>);

impl ClaimedPacketLeaf for PacketOnlyLeaf {
    fn packet_flow_adapter(&self) -> Option<&crate::runtime::packet_flow::PacketFlowAdapter> {
        self.0.as_ref()
    }
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
        Some(Arc::new(PacketOnlyLeaf(None))),
    );
    assert!(leaf.prepare_tcp_connect(None).is_err());
    assert!(leaf.prepare_udp_flow(None).is_err());
    for mode in [
        zero_engine::RouteMode::Auto,
        zero_engine::RouteMode::Packet,
        zero_engine::RouteMode::Flow,
    ] {
        assert!(leaf.prepare_tcp_connect_for_route(None, mode).is_err());
        assert!(leaf.prepare_udp_flow_for_route(None, mode).is_err());
    }
    let path = NetworkGraph::packet_ingress()
        .shortest_path(Plane::Packet, leaf.data_plane_sinks(), None)
        .expect("packet-only outbound remains routable");
    assert_eq!(path.planes, [Plane::Packet]);
}

#[tokio::test]
async fn packet_only_leaf_uses_opt_in_converter_without_native_l4_hooks() {
    let fixture = crate::runtime::packet_flow::tests::support::Fixture::new(1420);
    let leaf = ClaimedOutboundLeaf::new(
        OutboundLeafRuntime::virtual_outbound(
            "packet-only",
            "packet-only",
            TcpPathCategory::Session,
        ),
        ClaimedTcpHooks::default(),
        ClaimedUdpHooks::default(),
        Some(Arc::new(PacketOnlyLeaf(Some(fixture.adapter.clone())))),
    );
    assert!(!leaf.has_tcp_capability());
    assert!(!leaf.has_udp_flow_capability());
    for mode in [
        zero_engine::RouteMode::Auto,
        zero_engine::RouteMode::Packet,
        zero_engine::RouteMode::Flow,
        zero_engine::RouteMode::Translate,
    ] {
        assert!(leaf.prepare_tcp_connect_for_route(None, mode).is_ok());
        assert!(leaf.prepare_udp_flow_for_route(None, mode).is_ok());
    }
}

#[tokio::test]
async fn auto_keeps_native_flow_priority_and_packet_mode_prepares_only_the_converter() {
    use crate::protocol_registry::ClaimedTcpOutboundLeaf;
    use crate::runtime::packet_flow::PacketFlowAdapter;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Native {
        calls: Arc<AtomicUsize>,
        flow: PacketFlowAdapter,
    }
    impl ClaimedTcpOutboundLeaf<'_> for Native {
        fn prepare_tcp_connect(
            &self,
            _: Option<&std::path::Path>,
        ) -> Result<
            Box<dyn crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation>,
            crate::transport::TcpOutboundFailure,
        > {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.flow.prepare_tcp())
        }
    }
    let fixture = crate::runtime::packet_flow::tests::support::Fixture::new(1420);
    let calls = Arc::new(AtomicUsize::new(0));
    let leaf = ClaimedOutboundLeaf::new(
        OutboundLeafRuntime::virtual_outbound("both", "both", TcpPathCategory::Session),
        ClaimedTcpHooks {
            capability: Some(Arc::new(Native {
                calls: calls.clone(),
                flow: fixture.adapter.clone(),
            })),
        },
        ClaimedUdpHooks::default(),
        Some(Arc::new(PacketOnlyLeaf(Some(fixture.adapter.clone())))),
    );
    assert!(leaf
        .prepare_tcp_connect_for_route(None, zero_engine::RouteMode::Auto)
        .is_ok());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(leaf
        .prepare_tcp_connect_for_route(None, zero_engine::RouteMode::Packet)
        .is_ok());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
