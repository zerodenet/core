mod datagram;
pub(crate) use datagram::UdpAssociationObserver;
use zero_engine::Engine;

use super::{TcpExecutionServices, TcpRuntimeServices, UpstreamConnectServices};
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

mod packet_path;
pub(crate) use packet_path::PacketPathExecutionServices;
mod network;
#[derive(Clone, Copy)]
pub(crate) enum UdpAssociationCloseKind {
    Closed,
    IdleTimeout,
    Dropped,
}

impl UdpRuntimeServices {
    pub(crate) fn with_outbound_io(mut self, tag: &str) -> Self {
        let observer = crate::runtime::traffic_io::outbound(
            self.tcp.engine(),
            tag,
            zero_api::TrafficPlane::Outer,
        );
        self.tcp = self.tcp.with_io_observer(observer.clone());
        self.network.upstream = self.network.upstream.with_observer(observer);
        self
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn outbound_inner_io(
        &self,
        tag: &str,
    ) -> Option<std::sync::Arc<dyn zero_traits::IoObserver>> {
        crate::runtime::traffic_io::outbound(self.tcp.engine(), tag, zero_api::TrafficPlane::Inner)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn bind_outbound_peer_flow(&self, session_id: u64, tag: &str, peer_id: &str) {
        self.tcp
            .engine
            .bind_session_peer(session_id, tag, true, peer_id);
    }
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

    pub(crate) fn packet_path_execution(&self) -> PacketPathExecutionServices {
        PacketPathExecutionServices {
            driver: None,
            packet_target: None,
            receive: None,
            tcp: self.tcp.execution(),
            network: self.network(),
        }
    }

    pub(crate) async fn resolve_node_address(
        &self,
        address: &zero_core::Address,
        port: u16,
        error_message: &'static str,
    ) -> Result<std::net::SocketAddr, zero_engine::EngineError> {
        self.network
            .resolve_node_address(address, port, error_message)
            .await
    }

    pub(crate) async fn resolve_direct_targets(
        &self,
        session: &zero_core::Session,
        policy: &zero_traits::DialPolicy,
    ) -> Result<crate::transport::DirectTargetResolution, zero_engine::EngineError> {
        self.tcp
            .upstream
            .connector
            .resolve_target_addrs_with_policy(
                session,
                self.tcp.upstream.resolver.as_ref(),
                &self.tcp.upstream.egress_interface,
                policy,
            )
            .await
            .map_err(Into::into)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn resolve_packet_targets(
        &self,
        session: &zero_core::Session,
    ) -> Result<Vec<std::net::SocketAddr>, zero_engine::EngineError> {
        crate::runtime::resolve_packet_targets(session, self.tcp.upstream.resolver.as_ref()).await
    }

    pub(crate) fn direct_udp_network_observation(
        &self,
        resolution: &crate::transport::DirectTargetResolution,
        remote: std::net::SocketAddr,
        local: Option<std::net::SocketAddr>,
        selection: &zero_platform_tokio::EgressSelection,
    ) -> zero_engine::FlowNetworkObservation {
        self.tcp
            .upstream
            .connector
            .udp_network_observation_with_policy(resolution, remote, local, selection)
    }

    pub(crate) fn direct_resolution_failure_observation(
        &self,
        session: &zero_core::Session,
        policy: &zero_traits::DialPolicy,
    ) -> zero_engine::FlowNetworkObservation {
        self.tcp
            .upstream
            .connector
            .resolution_failure_observation_with_policy(
                session,
                self.tcp.upstream.resolver.as_ref(),
                &self.tcp.upstream.egress_interface,
                policy,
            )
    }

    pub(crate) fn record_session_network(
        &self,
        session_id: u64,
        network: zero_engine::FlowNetworkObservation,
    ) {
        self.tcp
            .engine()
            .record_session_network(session_id, network);
    }

    pub(crate) async fn dispatch_prepared_tcp_relay_carrier(
        &self,
        prepared: crate::inventory::PreparedTcpRelayChain,
    ) -> Result<crate::transport::RelayCarrier, crate::transport::TcpOutboundFailure> {
        self.tcp.dispatch_prepared_tcp_relay_carrier(prepared).await
    }

    pub(crate) fn prepare_lazy_tcp_relay_carrier<'a>(
        &self,
        prepared: crate::inventory::PreparedTcpRelayChain,
    ) -> crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'a> {
        self.tcp.prepare_lazy_tcp_relay_carrier(prepared)
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
