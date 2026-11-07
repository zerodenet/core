//! Actual same-Source local resource ceilings and cold identity.
use super::*;
impl Options {
    pub fn source_limits(&self) -> InboundSourceLimits {
        self.source_limits
    }
    pub fn with_inbound_source_limits(
        mut self,
        limits: InboundSourceLimits,
    ) -> Result<Self, zero_platform_tokio::contract::ChannelError> {
        let limits = limits.validate()?;
        let used = {
            let mut used = vec![
                limits.accepted_sockets,
                limits.bind_attempts,
                limits.datagram_peers,
                limits.packets_per_peer,
                limits.pending_sends,
            ];
            if self.plugin.is_some() {
                used.push(limits.handoff_probe_attempts);
            }
            used
        };
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-source-limits-v1");
        for value in used {
            identity.extend_from_slice(&value.get().to_le_bytes());
        }
        self.identity = identity.into();
        self.source_limits = limits;
        Ok(self)
    }
}
