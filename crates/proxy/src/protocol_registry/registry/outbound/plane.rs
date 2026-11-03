//! Candidate-local plane selection for ordinary TCP and UDP flow inbounds.

use zero_engine::{EngineError, RouteMode};

use super::ClaimedOutboundLeaf;
use crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation;
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;

impl<'a> ClaimedOutboundLeaf<'a> {
    pub(crate) fn tcp_connect_for_route(
        &self,
        mode: RouteMode,
    ) -> Result<std::sync::Arc<dyn PreparedTcpConnectOperation>, crate::transport::TcpOutboundFailure>
    {
        #[cfg(feature = "raw-ip-runtime")]
        {
            use crate::runtime::network_graph::{NetworkGraph, Plane, PlaneSet};
            use zero_stack::packet::IPPROTO_TCP;

            let converted = self.packet.as_ref().and_then(|leaf| leaf.flow.as_ref());
            let mut sinks = PlaneSet::empty();
            if self.tcp.is_some() {
                sinks.insert(Plane::Stream);
            }
            if converted.is_some() {
                sinks.insert(Plane::Packet);
            }
            match mode {
                RouteMode::Auto => {}
                RouteMode::Packet => sinks.retain_only(Plane::Packet),
                // A Flow path may terminate at Packet through its executable
                // active-stack conversion; it need not have a native stream sink.
                RouteMode::Flow | RouteMode::Translate => {}
            }
            let path =
                NetworkGraph::flow_ingress().shortest_path(Plane::Stream, sinks, Some(IPPROTO_TCP));
            match path.as_ref().and_then(|path| path.planes.last()) {
                Some(Plane::Packet) => Ok(converted
                    .expect("packet conversion was advertised")
                    .prepare_tcp()
                    .into()),
                Some(Plane::Stream) => self.tcp_connect(),
                _ => Err(missing_tcp_route_capability(mode)),
            }
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            if mode == RouteMode::Packet {
                return Err(missing_tcp_route_capability(mode));
            }
            self.tcp_connect()
        }
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn into_udp_flow_for_route(
        self,
        mode: RouteMode,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, crate::runtime::udp_dispatch::FlowFailure>
    {
        #[cfg(feature = "raw-ip-runtime")]
        {
            use crate::runtime::network_graph::{NetworkGraph, Plane, PlaneSet};
            use zero_stack::packet::IPPROTO_UDP;

            let converted = self.packet.as_ref().and_then(|leaf| leaf.flow.as_ref());
            let mut sinks = PlaneSet::empty();
            if self.udp.is_some() {
                sinks.insert(Plane::Datagram);
            }
            if converted.is_some() {
                sinks.insert(Plane::Packet);
            }
            match mode {
                RouteMode::Auto => {}
                RouteMode::Packet => sinks.retain_only(Plane::Packet),
                RouteMode::Flow | RouteMode::Translate => {}
            }
            let path = NetworkGraph::flow_ingress().shortest_path(
                Plane::Datagram,
                sinks,
                Some(IPPROTO_UDP),
            );
            match path.as_ref().and_then(|path| path.planes.last()) {
                Some(Plane::Packet) => Ok(converted
                    .expect("packet conversion was advertised")
                    .prepare_udp()),
                Some(Plane::Datagram) => self.into_udp_flow(),
                _ => Err(missing_udp_route_capability(mode)),
            }
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            if mode == RouteMode::Packet {
                return Err(missing_udp_route_capability(mode));
            }
            self.into_udp_flow()
        }
    }
}

fn missing_tcp_route_capability(mode: RouteMode) -> crate::transport::TcpOutboundFailure {
    crate::transport::TcpOutboundFailure {
        stage: "data_plane_route",
        error: EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("no executable TCP path for route mode {mode:?}"),
        )),
        upstream_endpoint: None,
        network: None,
    }
}

#[cfg(feature = "udp-runtime")]
fn missing_udp_route_capability(mode: RouteMode) -> crate::runtime::udp_dispatch::FlowFailure {
    crate::runtime::udp_dispatch::FlowFailure {
        stage: "data_plane_route",
        error: EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("no executable UDP path for route mode {mode:?}"),
        )),
        upstream: None,
    }
}
