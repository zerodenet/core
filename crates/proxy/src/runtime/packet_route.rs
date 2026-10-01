//! Packet-plane execution contract, independent of TCP/UDP flow operations.

use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::sync::mpsc;

mod session;
pub(crate) use session::{PacketPlane, PacketSessionPins};

/// Facts from the actual packet execution, with no protocol-private state.
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
    async fn forward(
        &self,
        packet: Vec<u8>,
        ingress_id: u64,
        replies: mpsc::Sender<Vec<u8>>,
        egress_generation: u64,
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
    ) -> io::Result<Vec<u8>>;
}
