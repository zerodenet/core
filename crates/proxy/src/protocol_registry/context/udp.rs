use zero_engine::Engine;

use super::{TcpRuntimeServices, UpstreamConnectServices};
use crate::inventory::ProtocolInventory;

#[derive(Clone)]
pub(crate) struct UdpRuntimeServices {
    tcp: TcpRuntimeServices,
    network: UdpNetworkServices,
}

#[derive(Clone)]
pub(crate) struct UdpNetworkServices {
    upstream: UpstreamConnectServices,
    engine: Engine,
}

#[derive(Clone, Copy)]
pub(crate) enum UdpAssociationCloseKind {
    Closed,
    IdleTimeout,
    Dropped,
}

impl UdpRuntimeServices {
    pub(crate) fn new(tcp: TcpRuntimeServices) -> Self {
        let network = UdpNetworkServices {
            upstream: tcp.upstream(),
            engine: tcp.engine.clone(),
        };
        Self { tcp, network }
    }

    pub(crate) fn protocols(&self) -> &ProtocolInventory {
        self.tcp.protocols()
    }

    pub(crate) fn upstream(&self) -> UpstreamConnectServices {
        self.tcp.upstream()
    }

    pub(crate) fn network(&self) -> UdpNetworkServices {
        self.network.clone()
    }

    pub(crate) async fn resolve_direct_address(
        &self,
        address: &zero_core::Address,
        port: u16,
        error_message: &'static str,
    ) -> Result<std::net::SocketAddr, zero_engine::EngineError> {
        self.network
            .resolve_direct_address(address, port, error_message)
            .await
    }

    pub(crate) async fn resolve_direct_target(
        &self,
        session: &zero_core::Session,
    ) -> Result<std::net::SocketAddr, zero_engine::EngineError> {
        self.tcp
            .upstream
            .protocols
            .direct_connector()
            .resolve_target_addr(session, self.tcp.upstream.resolver.as_ref())
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn dispatch_prepared_tcp_relay_carrier(
        &self,
        prepared: crate::inventory::PreparedTcpRelayChain<'_>,
    ) -> Result<crate::transport::RelayCarrier, crate::transport::TcpOutboundFailure> {
        self.tcp.dispatch_prepared_tcp_relay_carrier(prepared).await
    }

    pub(crate) fn record_session_inbound_rx(&self, session_id: u64, bytes: u64) {
        self.tcp.record_session_inbound_rx(session_id, bytes);
    }

    pub(crate) fn record_session_inbound_tx(&self, session_id: u64, bytes: u64) {
        self.tcp.record_session_inbound_tx(session_id, bytes);
    }

    pub(crate) fn record_session_outbound_rx(&self, session_id: u64, bytes: u64) {
        self.tcp.record_session_outbound_rx(session_id, bytes);
    }

    pub(crate) fn record_session_outbound_tx(&self, session_id: u64, bytes: u64) {
        self.tcp.record_session_outbound_tx(session_id, bytes);
    }

    #[cfg(feature = "udp-response-runtime")]
    pub(crate) fn record_session_inbound_traffic(
        &self,
        session_id: u64,
        traffic: zero_transport::StreamTraffic,
    ) {
        if traffic.is_empty() {
            return;
        }
        self.record_session_inbound_rx(session_id, traffic.read_bytes);
        self.record_session_inbound_tx(session_id, traffic.written_bytes);
    }

    pub(crate) fn udp_enabled_for_outbound(&self, outbound_tag: Option<&str>) -> bool {
        let config = self.tcp.config();
        config.runtime.udp.enabled
            && outbound_tag
                .and_then(|tag| config.outbounds.iter().find(|outbound| outbound.tag == tag))
                .map(|outbound| outbound.udp.enabled)
                .unwrap_or(true)
    }

    pub(crate) fn udp_upstream_idle_timeout(&self) -> std::time::Duration {
        self.tcp.engine().udp_upstream_idle_timeout()
    }

    pub(crate) fn record_udp_upstream_association_created(&self) {
        self.tcp.engine().record_udp_upstream_association_created();
    }

    pub(crate) fn record_udp_upstream_association_reused(&self) {
        self.tcp.engine().record_udp_upstream_association_reused();
    }

    pub(crate) fn record_udp_upstream_association_failed(&self) {
        self.tcp.engine().record_udp_upstream_association_failed();
    }

    pub(crate) fn record_udp_upstream_send_failure(&self) {
        self.tcp.engine().record_udp_upstream_send_failure();
    }

    pub(crate) fn record_udp_upstream_packet_sent(&self) {
        self.tcp.engine().record_udp_upstream_packet_sent();
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn record_udp_upstream_recv_failure(&self) {
        self.tcp.engine().record_udp_upstream_recv_failure();
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn record_udp_upstream_packet_received(&self) {
        self.tcp.engine().record_udp_upstream_packet_received();
    }
}

impl UdpNetworkServices {
    pub(crate) fn outbound_datagram_socket_factory(
        &self,
    ) -> zero_transport::OutboundDatagramSocketFactory {
        self.upstream.outbound_datagram_socket_factory()
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
            .protocols
            .direct_connector()
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

    pub(crate) async fn bind_datagram_socket(
        &self,
        peer: std::net::SocketAddr,
    ) -> Result<zero_platform_tokio::TokioDatagramSocket, zero_engine::EngineError> {
        self.upstream.bind_datagram_socket(peer).await
    }

    pub(crate) async fn resolve_direct_address(
        &self,
        address: &zero_core::Address,
        port: u16,
        error_message: &'static str,
    ) -> Result<std::net::SocketAddr, zero_engine::EngineError> {
        self.upstream
            .protocols
            .direct_connector()
            .resolve_address(
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
