//! Packet-plane execution contract, independent of TCP/UDP flow operations.

use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;

/// Execution ownership is shared across TUN and protocol raw-IP inbounds.
pub(crate) fn next_ingress_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub(crate) mod host;
mod session;
pub(crate) use session::{PacketPlane, PacketSessionPins};

/// Facts from the actual packet execution, with no protocol-private state.
#[derive(Debug)]
pub(crate) struct PacketForwardObservation {
    pub(crate) response: Option<Vec<u8>>,
    pub(crate) peer_identity: Option<std::sync::Arc<str>>,
}
impl PacketForwardObservation {
    pub(crate) fn local(response: Option<Vec<u8>>) -> Self {
        Self {
            response,
            peer_identity: None,
        }
    }
    pub(crate) fn forwarded(peer_identity: Option<std::sync::Arc<str>>) -> Self {
        Self {
            response: None,
            peer_identity,
        }
    }
}

#[async_trait]
pub(crate) trait PreparedPacketRouteOperation: Send + Sync {
    /// Forward one inner IP packet. A local L3 error may be returned for the
    /// ingress to emit; authenticated return packets use `replies`.
    /// On success the operation may take the buffer. A failure before device
    /// handoff retains the original packet for fallback. An empty error buffer
    /// means handoff could not be recovered and must not be retried.
    async fn forward(
        &self,
        packet: &mut zero_traits::PacketBuffer,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
        egress_generation: u64,
        observer: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<PacketForwardObservation>;
}

/// A bounded one-shot datagram exchange derived from a packet-capable outbound.
#[async_trait]
pub(crate) trait PreparedDatagramExchangeOperation: Send + Sync {
    async fn exchange(
        &self,
        endpoint: SocketAddr,
        payload: Vec<u8>,
        egress_generation: u64,
        observer: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<Vec<u8>>;
}
