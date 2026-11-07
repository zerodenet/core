//! Inert candidate projection for the one actual SS adapter's security owners.
//! This grants no IO, starts no worker and cannot install a limit on a live owner.
use zero_config::{ConfigError, RuntimeOptionsConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsSecurityBudgetLimits {
    pub tcp_salts: usize,
    pub legacy_bloom_bytes: usize,
    pub udp_receiver_windows: usize,
    pub udp_senders: usize,
    pub udp_associations: usize,
    pub udp_return_bindings: usize,
    pub udp_authenticated_codecs: usize,
}
impl SsSecurityBudgetLimits {
    pub fn from_runtime(runtime: &RuntimeOptionsConfig) -> Result<Self, ConfigError> {
        let limits = Self {
            tcp_salts: runtime.effective_ss_tcp_replay_max_salts(),
            legacy_bloom_bytes: runtime.effective_ss_legacy_replay_max_bytes(),
            udp_receiver_windows: runtime.effective_ss_udp_replay_max_windows(),
            udp_senders: runtime.effective_ss_udp_max_senders(),
            udp_associations: runtime.effective_ss_udp_max_associations(),
            udp_return_bindings: runtime.effective_ss_udp_max_return_bindings(),
            udp_authenticated_codecs: runtime.effective_ss_udp_max_authenticated_codecs(),
        };
        for (owner, value) in [
            ("TCP2022 salts", limits.tcp_salts),
            ("legacy Bloom bitmap bytes", limits.legacy_bloom_bytes),
            ("UDP receiver windows", limits.udp_receiver_windows),
            ("UDP sender identities/counters", limits.udp_senders),
            ("UDP authenticated associations", limits.udp_associations),
            ("UDP return bindings", limits.udp_return_bindings),
            ("UDP authenticated codecs", limits.udp_authenticated_codecs),
        ] {
            if value == 0 || value.checked_add(1).is_none() {
                return Err(ConfigError::InvalidRuntime(format!(
                    "SS {owner} budget must be positive and countable"
                )));
            }
        }
        Ok(limits)
    }
}
impl Default for SsSecurityBudgetLimits {
    fn default() -> Self {
        Self::from_runtime(&RuntimeOptionsConfig::default()).expect("valid SS security defaults")
    }
}

#[cfg(test)]
#[path = "../tests/ss_security_budget.rs"]
mod tests;
