use async_trait::async_trait;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use zero_core::Address;
use zero_engine::EngineError;

/// Object-safe packet-path carrier.
///
/// Each concrete carrier implements this so the packet-path manager can hold a
/// `Arc<dyn PacketPathCarrier>` without a per-protocol enum. Adapters build the
/// concrete carrier and box it; adding a carrier = implement this trait + the
/// adapter's prepared packet-path operation, zero manager changes.
#[async_trait]
pub(crate) trait PacketPathCarrier: Send + Sync {
    /// Send `payload` to `target:port` through this carrier.
    async fn send_to(&self, target: &Address, port: u16, payload: &[u8])
        -> Result<(), EngineError>;

    /// Receive the next datagram, stripping transport framing.
    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, EngineError>;

    /// Return the source reported by an address-bearing packet path, when one
    /// is available. Opaque connected transports have no source to report.
    async fn recv_from_with_source(
        &self,
        buf: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        self.recv_from(buf).await.map(|size| (size, None))
    }
}

pub(crate) fn packet_path_source(address: &Address, port: u16) -> Option<SocketAddr> {
    let ip = match address {
        Address::Ipv4(octets) => IpAddr::V4(Ipv4Addr::from(*octets)),
        Address::Ipv6(octets) => IpAddr::V6(Ipv6Addr::from(*octets)),
        Address::Domain(_) => return None,
    };
    Some(SocketAddr::new(ip, port))
}

/// Generic payload-based packet-path transport bridge.
///
/// Some protocol adapters expose an already-established upstream transport that
/// can send target-addressed payloads and return stripped payloads directly.
/// Runtime wraps those transports into a neutral `PacketPathCarrier` so
/// adapters do not need to define one-off carrier structs.
#[async_trait]
#[cfg(feature = "upstream-association-runtime")]
pub(crate) trait PacketPathPayloadTransport: Send + Sync {
    async fn send_to(&self, target: &Address, port: u16, payload: &[u8])
        -> Result<(), EngineError>;

    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, EngineError>;

    async fn recv_from_with_source(
        &self,
        buf: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        self.recv_from(buf).await.map(|size| (size, None))
    }
}

#[cfg(feature = "upstream-association-runtime")]
struct PacketPathPayloadCarrier {
    transport: Arc<dyn PacketPathPayloadTransport>,
    services: crate::protocol_registry::UdpNetworkServices,
}

#[cfg(feature = "upstream-association-runtime")]
impl Drop for PacketPathPayloadCarrier {
    fn drop(&mut self) {
        self.services
            .record_association_close(crate::protocol_registry::UdpAssociationCloseKind::Closed);
    }
}

#[async_trait]
#[cfg(feature = "upstream-association-runtime")]
impl PacketPathCarrier for PacketPathPayloadCarrier {
    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        self.transport.send_to(target, port, payload).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, EngineError> {
        self.transport.recv_from(buf).await
    }

    async fn recv_from_with_source(
        &self,
        buf: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        self.transport.recv_from_with_source(buf).await
    }
}

#[cfg(feature = "upstream-association-runtime")]
pub(crate) fn packet_path_payload_carrier(
    services: crate::protocol_registry::UdpNetworkServices,
    transport: Arc<dyn PacketPathPayloadTransport>,
) -> Arc<dyn PacketPathCarrier> {
    services.record_association_created();
    Arc::new(PacketPathPayloadCarrier {
        transport,
        services,
    })
}

/// Carrier identity for cache lookup (cheap, computed before dialing).
///
/// Produced by `PreparedUdpPacketPathOperation::into_carrier_descriptor`. The
/// `cache_key` uniquely identifies one carrier connection so the manager can
/// reuse it across packets; `server`/`port` are the endpoint for diagnostics.
#[derive(Clone)]
pub(crate) struct PacketPathCarrierDescriptor {
    pub(crate) tag: Option<String>,
    pub(crate) cache_key: String,
    pub(crate) server: String,
    pub(crate) port: u16,
}

#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) fn packet_path_carrier_descriptor(
    cache_key: String,
    server: &str,
    port: u16,
) -> PacketPathCarrierDescriptor {
    PacketPathCarrierDescriptor {
        tag: None,
        cache_key,
        server: server.to_owned(),
        port,
    }
}

#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) trait PacketPathCarrierDescriptorBuild {
    fn into_parts(self) -> (String, String, u16);
}

#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) fn packet_path_carrier_descriptor_from_build(
    build: impl PacketPathCarrierDescriptorBuild,
) -> PacketPathCarrierDescriptor {
    let (cache_key, server, port) = build.into_parts();
    packet_path_carrier_descriptor(cache_key, &server, port)
}
