use std::net::SocketAddr;

use zero_engine::EngineError;

use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::sessions::UdpSessionFlows;
use crate::runtime::udp_flow::state::UdpFlowState;
use crate::runtime::udp_socket::DirectUdpSockets;

impl UdpDispatch {
    /// Create a dispatcher; Direct sockets are opened only when needed.
    pub(crate) async fn new(
        runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
        inbound_tag: &str,
        protocols: &crate::inventory::ProtocolInventory,
    ) -> Result<Self, EngineError> {
        let preferred_port = runtime.source_addr().map(|source| source.port());
        let direct_socket = DirectUdpSockets::new(runtime.services().network(), preferred_port);
        let (cancel_tx, cancel_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            runtime,
            inbound_tag: inbound_tag.to_owned(),
            flows: UdpSessionFlows::default(),
            flow_start_backoff: Default::default(),
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
        &mut self,
        session_id: u64,
        target_addr: SocketAddr,
        policy: &crate::runtime::udp_socket::DirectUdpPolicy,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        self.direct_socket
            .send_to_addr(payload, target_addr, session_id, policy)
            .await
    }

    pub(crate) async fn send_new_direct_packet(
        &mut self,
        session_id: u64,
        logical_target: &zero_core::Address,
        candidates: &[SocketAddr],
        policy: &crate::runtime::udp_socket::DirectUdpPolicy,
        payload: &[u8],
    ) -> Result<crate::runtime::udp_socket::DirectUdpSentPacket, EngineError> {
        let sent = self
            .direct_socket
            .send_new_packet(logical_target, candidates, payload, session_id, policy)
            .await?;
        tracing::debug!(
            target = %sent.target,
            egress_generation = self.direct_socket.generation(),
            candidate_count = candidates.len(),
            "selected direct UDP target"
        );
        Ok(sent)
    }
}
