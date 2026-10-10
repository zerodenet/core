//! Runtime-owned storage policy. Protocol identities remain opaque inputs.
use sha2::{Digest, Sha256};
use zero_stack::client_udp::ClientUdpReceiveQueueConfig;

#[derive(Clone, Default)]
pub(crate) struct RawIpStackOptions {
    pub(crate) udp_receive_queue: ClientUdpReceiveQueueConfig,
    pub(crate) traffic: super::RawIpTraffic,
}
impl RawIpStackOptions {
    pub(crate) fn from_config(config: &zero_config::UdpReceiveQueueConfig) -> Self {
        Self {
            udp_receive_queue: ClientUdpReceiveQueueConfig {
                capacity: config.capacity,
                max_payload_bytes: config.max_payload_bytes,
                max_stack_payload_bytes: config.max_stack_payload_bytes,
            },
            traffic: Default::default(),
        }
    }
    /// Changing stack limits participates in ordinary transactional device
    /// replacement; an unchanged policy retains the shared devices/flows.
    pub(crate) fn identity(&self, protocol_identity: [u8; 32]) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"zero.raw-ip-stack.udp-queue.v1");
        hash.update(protocol_identity);
        for value in [
            self.udp_receive_queue.capacity,
            self.udp_receive_queue.max_payload_bytes.unwrap_or(0),
            self.udp_receive_queue.max_stack_payload_bytes.unwrap_or(0),
        ] {
            hash.update((value as u64).to_le_bytes());
        }
        hash.finalize().into()
    }
    pub(crate) fn with_traffic(mut self, traffic: super::RawIpTraffic) -> Self {
        self.traffic = traffic;
        self
    }
}
