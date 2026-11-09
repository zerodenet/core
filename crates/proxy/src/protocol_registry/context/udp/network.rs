//! The original UDP network projections, split without changing ownership.
use super::*;
impl UdpNetworkServices {
    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) fn upstream_idle_timeout(&self) -> std::time::Duration {
        self.engine.udp_upstream_idle_timeout()
    }

    pub(crate) fn from_tcp_execution(services: &super::TcpExecutionServices) -> Self {
        Self {
            upstream: services.upstream(),
            engine: services.engine().clone(),
        }
    }

    pub(crate) fn outbound_datagram_socket_factory(
        &self,
    ) -> zero_transport::OutboundDatagramSocketFactory {
        self.upstream.outbound_datagram_socket_factory()
    }

    pub(crate) fn egress_generation(&self) -> u64 {
        self.upstream.egress_interface.generation()
    }

    pub(crate) fn direct_policy_is_current(
        &self,
        policy: &crate::runtime::udp_socket::DirectUdpPolicy,
    ) -> bool {
        self.engine.runtime_snapshot().direct_dial_policy_matches(
            policy.tag.as_deref(),
            &policy.dial_policy,
            policy.generation,
        )
    }

    pub(crate) fn ensure_direct_policy_current(
        &self,
        policy: &crate::runtime::udp_socket::DirectUdpPolicy,
    ) -> Result<(), zero_engine::EngineError> {
        if self.direct_policy_is_current(policy) {
            Ok(())
        } else {
            Err(zero_engine::EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "direct UDP dial policy changed while this flow was active",
            )))
        }
    }

    pub(crate) fn direct_datagram_selection(
        &self,
        peer: std::net::SocketAddr,
        policy: &zero_traits::DialPolicy,
    ) -> Result<zero_platform_tokio::EgressSelection, zero_engine::EngineError> {
        self.upstream
            .egress_interface
            .select_for_peer_with_policy(peer, policy)
            .map_err(Into::into)
    }

    pub(crate) async fn connect_upstream(
        &self,
        server: &str,
        port: u16,
    ) -> Result<zero_platform_tokio::TokioSocket, zero_transport::RuntimeError> {
        self.upstream.connect_upstream(server, port).await
    }

    pub(crate) async fn resolve_udp_peer(
        &self,
        address: &zero_core::Address,
        port: u16,
        context: &'static str,
    ) -> Result<
        (
            zero_traits::SocketAddress,
            zero_platform_tokio::TokioDatagramSocket,
        ),
        zero_transport::RuntimeError,
    > {
        let peer = self
            .upstream
            .connector
            .resolve_address(address, port, self.upstream.resolver.as_ref(), context)
            .await
            .map_err(zero_transport::RuntimeError::from)?;
        let socket = self
            .upstream
            .bind_datagram_socket(peer)
            .await
            .map_err(zero_transport::RuntimeError::from)?;
        Ok((
            zero_platform_tokio::socket_addr_to_socket_address(peer),
            socket,
        ))
    }

    pub(crate) async fn bind_direct_datagram_socket(
        &self,
        peer: std::net::SocketAddr,
        preferred_port: Option<u16>,
        policy: &zero_traits::DialPolicy,
        selection: &zero_platform_tokio::EgressSelection,
    ) -> Result<zero_platform_tokio::TokioDatagramSocket, zero_engine::EngineError> {
        zero_platform_tokio::TokioDatagramSocket::bind_for_peer_with_policy_preserving_port(
            peer,
            preferred_port,
            policy,
            selection,
        )
        .await
        .map_err(Into::into)
    }

    pub(crate) fn direct_datagram_observer(
        &self,
    ) -> Option<std::sync::Arc<dyn zero_traits::IoObserver>> {
        self.upstream.observer()
    }

    pub(crate) async fn resolve_node_address(
        &self,
        address: &zero_core::Address,
        port: u16,
        error_message: &'static str,
    ) -> Result<std::net::SocketAddr, zero_engine::EngineError> {
        self.upstream
            .connector
            .resolve_node_address(
                address,
                port,
                self.upstream.resolver.as_ref(),
                error_message,
            )
            .await
            .map_err(Into::into)
    }

    pub(crate) fn record_control_traffic(
        &self,
        session_id: u64,
        traffic: zero_transport::StreamTraffic,
    ) {
        if traffic.is_empty() {
            return;
        }
        self.engine
            .record_session_outbound_rx(session_id, traffic.read_bytes);
        self.engine
            .record_session_outbound_tx(session_id, traffic.written_bytes);
    }

    pub(crate) fn record_association_created(&self) {
        self.engine.record_udp_upstream_association_created();
    }

    pub(crate) fn record_association_close(&self, kind: UdpAssociationCloseKind) {
        match kind {
            UdpAssociationCloseKind::Closed => self.engine.record_udp_upstream_association_closed(),
            UdpAssociationCloseKind::IdleTimeout => {
                self.engine.record_udp_upstream_association_idle_timeout()
            }
            UdpAssociationCloseKind::Dropped => {
                self.engine.record_udp_upstream_association_dropped()
            }
        }
    }

    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) async fn build_udp_socket_carrier(
        &self,
        server: &str,
        port: u16,
        codec: std::sync::Arc<
            dyn zero_traits::DatagramCodec<zero_core::Address, Error = zero_core::Error>,
        >,
    ) -> Result<
        std::sync::Arc<dyn crate::runtime::udp_flow::packet_path::PacketPathCarrier>,
        zero_engine::EngineError,
    > {
        crate::runtime::udp_flow::packet_path_chain::carriers::udp_socket_carrier::build(
            self, server, port, codec,
        )
        .await
    }
}
