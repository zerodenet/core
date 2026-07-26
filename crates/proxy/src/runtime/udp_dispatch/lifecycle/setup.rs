use std::net::SocketAddr;

use zero_engine::EngineError;
use zero_platform_tokio::TokioDatagramSocket;

use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::sessions::UdpSessionFlows;
use crate::runtime::udp_flow::state::UdpFlowState;
use crate::runtime::udp_socket::send_direct_udp_packet;

impl UdpDispatch {
    /// Create a new dispatcher with an ephemeral direct socket.
    pub(crate) async fn new(
        runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
        inbound_tag: &str,
        protocols: &crate::inventory::ProtocolInventory,
    ) -> Result<Self, EngineError> {
        let direct_socket = TokioDatagramSocket::bind("0.0.0.0:0").await?;
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
        send_direct_udp_packet(&self.direct_socket, target_addr, payload).await
    }
}
