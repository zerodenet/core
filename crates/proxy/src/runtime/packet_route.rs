//! Packet-plane execution contract, independent of TCP/UDP flow operations.

use std::io;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::sync::mpsc;

mod session;
pub(crate) use session::{PacketPlane, PacketSessionPins};

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
    ) -> io::Result<Option<Vec<u8>>>;
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
