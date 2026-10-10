//! Neutral Packet-to-Flow UDP reply queue limits, not quotas or wire pacing.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UdpReceiveQueueConfig {
    /// Events per socket, including ICMP errors. Existing default: 64.
    #[serde(default = "default_capacity")]
    pub capacity: usize,
    /// Queued UDP payload bytes per socket. Null keeps the slot-only bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_payload_bytes: Option<usize>,
    /// Queued payload bytes across sockets in ONE client stack. Null disables
    /// this additional bound; it is not an endpoint-wide or process-wide cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_stack_payload_bytes: Option<usize>,
}
impl Default for UdpReceiveQueueConfig {
    fn default() -> Self {
        Self {
            capacity: default_capacity(),
            max_payload_bytes: None,
            max_stack_payload_bytes: None,
        }
    }
}
const fn default_capacity() -> usize {
    64
}
impl UdpReceiveQueueConfig {
    pub(crate) fn validate(&self) -> Result<(), crate::ConfigError> {
        let name = "runtime.network.udp_receive_queue";
        if !(1..=65_536).contains(&self.capacity) {
            return Err(crate::ConfigError::InvalidRuntime(format!(
                "{name}.capacity must be between 1 and 65536"
            )));
        }
        // Every supported non-jumbo IPv4/IPv6 UDP datagram must fit by itself.
        for (field, bytes) in [
            ("max_payload_bytes", self.max_payload_bytes),
            ("max_stack_payload_bytes", self.max_stack_payload_bytes),
        ] {
            if bytes.is_some_and(|bytes| bytes < 65_527) {
                return Err(crate::ConfigError::InvalidRuntime(format!(
                    "{name}.{field} must be at least 65527 bytes or null"
                )));
            }
        }
        Ok(())
    }
}
