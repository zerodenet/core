use std::net::SocketAddr;

use zero_engine::EngineError;

use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::sessions::UdpSessionFlows;
use crate::runtime::udp_flow::state::UdpFlowState;
use crate::runtime::udp_socket::DirectUdpSockets;

impl UdpDispatch {
    /// Create a new dispatcher with an ephemeral direct socket.
    pub(crate) async fn new(
        runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
        inbound_tag: &str,
        protocols: &crate::inventory::ProtocolInventory,
    ) -> Result<Self, EngineError> {
        let direct_socket = DirectUdpSockets::bind(&runtime.services().network()).await?;
        let (cancel_tx, cancel_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            runtime,
            inbound_tag: inbound_tag.to_owned(),
            flows: UdpSessionFlows::default(),
            direct_socket,
            flow_state: UdpFlowState::new(protocols.registered_udp_handlers()),
            cancel_tx,
            cancel_rx,
        })
    }

    pub(crate) fn inbound_tag(&self) -> &str {
        &self.inbound_tag
    }

    /// Send a direct UDP packet through the dispatch-owned socket.
    pub(crate) async fn send_direct_packet(
        &self,
        target_addr: SocketAddr,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        self.direct_socket.send_to_addr(payload, target_addr).await
    }
}
