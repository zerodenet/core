//! SOCKS5 upstream UDP association runtime and transport bridges.

use ::socks5::transport::{
    establish_packet_path_udp_association, establish_registered_udp_association,
    Socks5ManagedUdpAssociationTarget, Socks5ManagedUdpPacketPathCarrierBuild,
    Socks5UdpAssociationRuntime, Socks5UpstreamAssociationCloseReason,
    Socks5UpstreamUdpAssociation,
};
use zero_core::Address;
use zero_engine::EngineError;
use zero_transport::RuntimeError;

use crate::protocol_registry::{UdpAssociationCloseKind, UdpNetworkServices};

#[derive(Clone)]
struct ProxySocks5UdpAssociationRuntime {
    services: UdpNetworkServices,
    record_associations: bool,
}

impl ProxySocks5UdpAssociationRuntime {
    fn new(services: UdpNetworkServices, record_associations: bool) -> Self {
        Self {
            services,
            record_associations,
        }
    }
}

#[async_trait::async_trait]
impl Socks5UdpAssociationRuntime for ProxySocks5UdpAssociationRuntime {
    async fn open_control_socket(
        &self,
        server: &str,
        port: u16,
    ) -> Result<zero_platform_tokio::TokioSocket, RuntimeError> {
        self.services.connect_upstream(server, port).await
    }

    async fn resolve_udp_relay(
        &self,
        relay_address: Address,
        relay_port: u16,
    ) -> Result<
        (
            zero_traits::SocketAddress,
            zero_platform_tokio::TokioDatagramSocket,
        ),
        RuntimeError,
    > {
        self.services
            .resolve_udp_peer(
                &relay_address,
                relay_port,
                "failed to resolve upstream socks5 udp relay",
            )
            .await
    }

    fn record_control_traffic(
        &self,
        session_id: u64,
        control: &mut crate::transport::MeteredStream<zero_platform_tokio::TokioSocket>,
    ) {
        self.services
            .record_control_traffic(session_id, control.drain_traffic());
    }

    fn record_close(&self, reason: Socks5UpstreamAssociationCloseReason) {
        if !self.record_associations {
            return;
        }
        match reason {
            Socks5UpstreamAssociationCloseReason::Closed => {
                self.services
                    .record_association_close(UdpAssociationCloseKind::Closed);
            }
            Socks5UpstreamAssociationCloseReason::IdleTimeout => {
                self.services
                    .record_association_close(UdpAssociationCloseKind::IdleTimeout);
            }
            Socks5UpstreamAssociationCloseReason::Dropped => {
                self.services
                    .record_association_close(UdpAssociationCloseKind::Dropped);
            }
        }
    }
}

pub(super) async fn establish_packet_path_association(
    services: UdpNetworkServices,
    build: Socks5ManagedUdpPacketPathCarrierBuild,
) -> Result<Socks5UpstreamUdpAssociation, EngineError> {
    // Packet-path lifecycle is tracked by the runtime's carrier wrapper.
    establish_packet_path_udp_association(
        ProxySocks5UdpAssociationRuntime::new(services, false),
        build,
        0,
    )
    .await
    .map_err(Into::into)
}

#[async_trait::async_trait]
impl crate::runtime::udp_flow::packet_path::PacketPathPayloadTransport
    for Socks5UpstreamUdpAssociation
{
    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        self.send_packet(target, port, payload)
            .await
            .map_err(EngineError::from)?;
        Ok(())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, EngineError> {
        self.recv_payload(buf).await.map_err(Into::into)
    }
}

#[async_trait::async_trait]
impl
    crate::runtime::udp_flow::registered::UpstreamAssociationTransport<
        Socks5ManagedUdpAssociationTarget,
    > for Socks5UpstreamUdpAssociation
{
    async fn establish(
        services: UdpNetworkServices,
        target: Socks5ManagedUdpAssociationTarget,
        session_id: u64,
    ) -> Result<Self, EngineError> {
        establish_registered_udp_association(
            ProxySocks5UdpAssociationRuntime::new(services, true),
            target,
            session_id,
        )
        .await
        .map_err(Into::into)
    }

    async fn send_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        self.send_packet(target, port, payload)
            .await
            .map_err(Into::into)
    }

    async fn recv_response_parts(
        &self,
        buf: &mut [u8],
    ) -> Result<(Address, u16, Vec<u8>), EngineError> {
        self.recv_response_parts(buf).await.map_err(Into::into)
    }

    fn close(self, reason: crate::runtime::udp_flow::registered::UpstreamAssociationCloseReason) {
        let reason = match reason {
            crate::runtime::udp_flow::registered::UpstreamAssociationCloseReason::Closed => {
                Socks5UpstreamAssociationCloseReason::Closed
            }
            crate::runtime::udp_flow::registered::UpstreamAssociationCloseReason::IdleTimeout => {
                Socks5UpstreamAssociationCloseReason::IdleTimeout
            }
            crate::runtime::udp_flow::registered::UpstreamAssociationCloseReason::Dropped => {
                Socks5UpstreamAssociationCloseReason::Dropped
            }
        };
        self.close(reason);
    }
}
