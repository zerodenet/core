//! Legacy L4 claims never replace the actual standard Packet provider.
use super::ClaimedOutboundLeaf;
use zero_engine::RouteMode;
impl<'a> ClaimedOutboundLeaf<'a> {
    pub(crate) fn tcp_connect_for_route(
        &self,
        mode: RouteMode,
    ) -> Result<
        std::sync::Arc<dyn crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation>,
        crate::transport::TcpOutboundFailure,
    > {
        if mode == RouteMode::Packet {
            return Err(crate::runtime::path::missing_tcp_route_capability(mode));
        }
        self.tcp_connect()
    }
    #[cfg(feature = "udp-runtime")]
    pub(crate) fn into_udp_flow_for_route(
        self,
        mode: RouteMode,
    ) -> Result<
        Box<dyn crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation + 'a>,
        crate::runtime::udp_dispatch::FlowFailure,
    > {
        if mode == RouteMode::Packet {
            return Err(crate::runtime::path::missing_udp_route_capability(mode));
        }
        self.into_udp_flow()
    }
}
