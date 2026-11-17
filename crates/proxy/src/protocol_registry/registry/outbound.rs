//! Optional cold carrier metadata; executable leaves use actual Host roles.
use super::{ProtocolRegistry, RegisteredProtocolEntry};
use zero_engine::EngineError;
#[cfg(feature = "udp-runtime")]
mod formatter;
#[cfg(feature = "udp-runtime")]
pub(crate) use formatter::ColdPacketPathProjection;
impl ProtocolRegistry {
    fn outbound_protocol_entry(&self, protocol: &str) -> Option<&RegisteredProtocolEntry> {
        self.entries
            .iter()
            .find(|entry| entry.support.name() == protocol)
    }
}
