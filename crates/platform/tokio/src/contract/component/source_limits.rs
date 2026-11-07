//! Concrete cold Source maxima, not a resource grant or a new executor.
use crate::contract::{ChannelError, Impact, Reason, Stage};
use std::num::NonZeroUsize;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundSourceLimits {
    pub accepted_sockets: NonZeroUsize,
    pub datagram_associations: NonZeroUsize,
    pub datagram_peers: NonZeroUsize,
    pub packets_per_peer: NonZeroUsize,
    pub pending_sends: NonZeroUsize,
    pub bind_attempts: NonZeroUsize,
    pub target_sockets: NonZeroUsize,
    pub file_sessions: NonZeroUsize,
    pub handoff_probe_attempts: NonZeroUsize,
}
impl Default for InboundSourceLimits {
    fn default() -> Self {
        Self {
            accepted_sockets: NonZeroUsize::new(4096).unwrap(),
            datagram_associations: NonZeroUsize::new(4096).unwrap(),
            datagram_peers: NonZeroUsize::new(128).unwrap(),
            packets_per_peer: NonZeroUsize::new(256).unwrap(),
            pending_sends: NonZeroUsize::new(32).unwrap(),
            bind_attempts: NonZeroUsize::new(50).unwrap(),
            target_sockets: NonZeroUsize::new(4096).unwrap(),
            file_sessions: NonZeroUsize::new(16).unwrap(),
            handoff_probe_attempts: NonZeroUsize::new(1000).unwrap(),
        }
    }
}
impl InboundSourceLimits {
    pub fn validate(self) -> Result<Self, ChannelError> {
        if [
            (self.accepted_sockets.get(), 65536),
            (self.datagram_associations.get(), 65536),
            (self.datagram_peers.get(), 65536),
            (self.packets_per_peer.get(), 65536),
            (self.pending_sends.get(), 65536),
            (self.bind_attempts.get(), 4096),
            (self.target_sockets.get(), 65536),
            (self.file_sessions.get(), 4096),
            (self.handoff_probe_attempts.get(), 65536),
        ]
        .into_iter()
        .any(|(actual, maximum)| actual > maximum)
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Component,
            ));
        }
        Ok(self)
    }
}
