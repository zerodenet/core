//! Native resource settings for protocol state; omitted capacity matches upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StateLimits {
    pub udp_capacity: Option<usize>,
    pub udp_timeout_secs: u64,
    pub tcp_replay_capacity: Option<usize>,
}
impl Default for StateLimits {
    fn default() -> Self {
        Self {
            udp_capacity: None,
            udp_timeout_secs: 300,
            tcp_replay_capacity: None,
        }
    }
}
impl StateLimits {
    pub fn validate(self) -> Result<(), alloc::string::String> {
        if self.udp_timeout_secs == 0
            || self.udp_capacity == Some(0)
            || self.tcp_replay_capacity == Some(0)
        {
            return Err("state timeout and configured capacities must be positive".into());
        }
        Ok(())
    }
    #[cfg(feature = "runtime")]
    pub(crate) fn udp_idle(self) -> core::time::Duration {
        core::time::Duration::from_secs(self.udp_timeout_secs)
    }
    #[cfg(all(feature = "runtime", feature = "blake3"))]
    pub(crate) fn udp_replay_retention(self) -> core::time::Duration {
        core::time::Duration::from_secs(self.udp_timeout_secs.max(61))
    }
}
