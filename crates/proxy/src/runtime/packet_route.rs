//! Packet-plane execution contract, independent of TCP/UDP flow operations.

/// Execution ownership is shared across TUN and protocol raw-IP inbounds.
#[cfg(test)]
pub(crate) fn next_ingress_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub(crate) mod host;
mod session;
mod standard;
pub(crate) use session::{PacketPlane, PacketSessionPins};
pub(crate) use standard::PacketRoleSessions;

/// Facts from the actual packet execution, with no protocol-private state.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct PacketForwardObservation {
    pub(crate) response: Option<Vec<u8>>,
    pub(crate) peer_identity: Option<std::sync::Arc<str>>,
}
#[cfg(test)]
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
