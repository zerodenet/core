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
        target_addr: SocketAddr,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        self.refresh_direct_sockets().await?;
        self.direct_socket.send_to_addr(payload, target_addr).await
    }

    pub(crate) async fn send_new_direct_packet(
        &mut self,
        logical_target: &zero_core::Address,
        candidates: &[SocketAddr],
        payload: &[u8],
    ) -> Result<(usize, SocketAddr), EngineError> {
        self.refresh_direct_sockets().await?;
        let target_addr = self.direct_socket.select_target(logical_target, candidates)?;
        let sent = self.direct_socket.send_to_addr(payload, target_addr).await?;
        tracing::debug!(
            target = %target_addr,
            egress_generation = self.direct_socket.generation(),
            candidate_count = candidates.len(),
            "selected direct UDP target"
        );
        Ok((sent, target_addr))
    }

    async fn refresh_direct_sockets(&mut self) -> Result<(), EngineError> {
        let network = self.runtime.services().network();
        self.direct_socket.refresh_if_stale(&network).await
    }
}
