//! Runtime-owned storage policy. Protocol identities remain opaque inputs.
use sha2::{Digest, Sha256};
use zero_stack::client_udp::ClientUdpReceiveQueueConfig;

#[derive(Clone)]
pub(crate) struct RawIpStackOptions {
    pub(crate) udp_receive_queue: ClientUdpReceiveQueueConfig,
    pub(crate) packet_wire_memory: Option<zero_stack::packet_output::memory::PacketWireMemory>,
    packet_memory_view:
        Option<std::sync::Arc<std::sync::OnceLock<zero_tun::packet_memory::PacketMemoryView>>>,
    pub(crate) packet_return_totals: std::sync::Arc<super::PacketReturnTotals>,
    pub(crate) packet_return_total_max_frames: usize,
    pub(crate) packet_return_total_max_bytes: usize,
    pub(crate) packet_return_max_frames: usize,
    pub(crate) packet_return_max_bytes: usize,
    pub(crate) packet_native_read_total_max_frames: usize,
    pub(crate) packet_native_read_total_max_bytes: usize,
    pub(crate) packet_transmit_total_max_frames: usize,
    pub(crate) packet_transmit_total_max_bytes: usize,
    pub(crate) traffic: super::RawIpTraffic,
}
impl RawIpStackOptions {
    pub(crate) fn from_config(config: &zero_config::UdpReceiveQueueConfig) -> Self {
        Self {
            packet_wire_memory: None,
            packet_memory_view: None,
            udp_receive_queue: ClientUdpReceiveQueueConfig {
                capacity: config.capacity,
                max_payload_bytes: config.max_payload_bytes,
                max_stack_payload_bytes: config.max_stack_payload_bytes,
            },
            packet_return_totals: Default::default(),
            packet_return_total_max_frames: 1024,
            packet_return_total_max_bytes: 64 * 1024 * 1024,
            packet_return_max_frames: 256,
            packet_return_max_bytes: 16 * 1024 * 1024,
            packet_native_read_total_max_frames: 256,
            packet_native_read_total_max_bytes: 16 * 1024 * 1024,
            packet_transmit_total_max_frames: 16384,
            packet_transmit_total_max_bytes: 64 * 1024 * 1024,
            traffic: Default::default(),
        }
    }
    pub(crate) fn with_packet_wire_memory(
        mut self,
        wire: zero_stack::packet_output::memory::PacketWireMemory,
    ) -> Self {
        self.packet_wire_memory = Some(wire);
        self
    }
    pub(crate) fn with_packet_memory_cell(
        mut self,
        cell: std::sync::Arc<std::sync::OnceLock<zero_tun::packet_memory::PacketMemoryView>>,
    ) -> Self {
        self.packet_memory_view = Some(cell);
        self
    }
    pub(super) fn packet_output(
        &self,
        sender: tokio::sync::mpsc::Sender<zero_stack::packet_output::ObservedPacket>,
        required: bool,
    ) -> Result<zero_stack::packet_output::PacketSender, zero_engine::EngineError> {
        let output = zero_stack::packet_output::PacketSender::from(sender);
        match self.packet_wire_memory {
            Some(wire) => match &self.packet_memory_view {
                Some(cell) => output
                    .with_packet_memory_cell(cell.clone(), wire)
                    .map_err(|source| zero_engine::EngineError::Io(std::io::Error::other(source))),
                None if !required => output
                    .with_packet_memory(self.packet_return_totals.clone(), wire)
                    .map_err(|source| zero_engine::EngineError::Io(std::io::Error::other(source))),
                None => Err(zero_engine::EngineError::Io(std::io::Error::other(
                    "actual Host packet output has no original allocation view binding",
                ))),
            },
            None if !required => Ok(output),
            None => Err(zero_engine::EngineError::Io(std::io::Error::other(
                "actual Host packet driver lacks its declared codec memory binding",
            ))),
        }
    }
    pub(crate) fn with_packet_native_read_limits(mut self, frames: usize, bytes: usize) -> Self {
        self.packet_native_read_total_max_frames = frames;
        self.packet_native_read_total_max_bytes = bytes;
        self
    }
    pub(crate) fn with_packet_transmit_limits(mut self, frames: usize, bytes: usize) -> Self {
        self.packet_transmit_total_max_frames = frames;
        self.packet_transmit_total_max_bytes = bytes;
        self
    }
    pub(crate) fn with_packet_return_limits(mut self, frames: usize, bytes: usize) -> Self {
        self.packet_return_max_frames = frames;
        self.packet_return_max_bytes = bytes;
        self
    }
    pub(crate) fn with_packet_return_totals(
        mut self,
        totals: std::sync::Arc<super::PacketReturnTotals>,
        frames: usize,
        bytes: usize,
    ) -> Self {
        self.packet_return_totals = totals;
        self.packet_return_total_max_frames = frames;
        self.packet_return_total_max_bytes = bytes;
        self
    }
    /// Changing stack limits participates in ordinary transactional device
    /// replacement; an unchanged policy retains the shared devices/flows.
    pub(crate) fn identity(&self, protocol_identity: [u8; 32]) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"zero.raw-ip-stack.udp-queue.v1");
        hash.update(protocol_identity);
        use zero_stack::packet_output::memory::PacketWireMemory;
        match self.packet_wire_memory {
            None => hash.update([0]),
            Some(PacketWireMemory::Native) => hash.update([1]),
            Some(PacketWireMemory::PaddedEnvelope { multiple, overhead }) => {
                hash.update([2]);
                hash.update(multiple.to_le_bytes());
                hash.update(overhead.to_le_bytes());
            }
        }

        for value in [
            self.packet_return_total_max_frames,
            self.packet_return_total_max_bytes,
            self.packet_return_max_frames,
            self.packet_return_max_bytes,
            self.packet_native_read_total_max_frames,
            self.packet_native_read_total_max_bytes,
            self.packet_transmit_total_max_frames,
            self.packet_transmit_total_max_bytes,
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

impl Default for RawIpStackOptions {
    fn default() -> Self {
        Self::from_config(&Default::default())
    }
}
