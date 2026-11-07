//! Actual same-original accepted/peer admission ceilings.
use super::*;
impl Options {
    pub(super) fn peer_limits(&self) -> DatagramPeerLimits {
        let mut limits = peer_limits();
        limits.max_tasks_per_peer = self.work_tasks;
        limits.max_resources_per_peer = self.work_resources;
        limits
    }

    /// Cold limits on this same original accepted/peer graph. No work starts.
    pub fn with_inbound_work_limits(
        mut self,
        tasks: NonZeroUsize,
        resources: usize,
        requests: NonZeroUsize,
    ) -> Result<Self, zero_platform_tokio::contract::ChannelError> {
        if tasks.get() > 4096 || resources > 65536 || requests.get() > 65536 {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-inbound-work-limits-v1");
        for n in [tasks.get(), resources, requests.get()] {
            identity.extend_from_slice(&n.to_le_bytes());
        }
        self.identity = identity.into();
        self.work_tasks = tasks;
        self.work_resources = resources;
        Ok(self)
    }
}
pub(super) fn peer_limits() -> DatagramPeerLimits {
    DatagramPeerLimits {
        max_tasks_per_peer: std::num::NonZeroUsize::new(1).unwrap(),
        max_resources_per_peer: 0,
        max_peers: NonZeroUsize::new(4096).unwrap(),
        packets_per_peer: NonZeroUsize::new(32).unwrap(),
    }
}
