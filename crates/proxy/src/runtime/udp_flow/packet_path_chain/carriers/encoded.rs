//! Compose an adapter-provided datagram codec over an already opened packet path.
use crate::runtime::udp_flow::packet_path::{
    packet_path_source, PacketPathCarrier, UdpDatagramSource,
};
use std::net::SocketAddr;
use std::sync::Arc;
use zero_core::Address;
use zero_engine::EngineError;

#[cfg(test)]
#[path = "tests/encoded.rs"]
mod tests;

/// Storage from the actual caller, with no implicit unbudgeted execution mode.
pub(crate) enum CodecMemory {
    #[cfg(feature = "peer-raw-ip-runtime")]
    Source(crate::protocol_registry::packet_memory::PacketCodecReceiveMemory),
    Business(crate::protocol_registry::packet_memory::business::BusinessMemory),
    #[cfg(test)]
    Library,
}
pub(crate) fn wrap(
    path: Arc<dyn PacketPathCarrier>,
    source: UdpDatagramSource,
    memory: CodecMemory,
) -> Arc<dyn PacketPathCarrier> {
    let endpoint = source.descriptor().endpoint();
    Arc::new(Encoded {
        path,
        source,
        target: endpoint.target(),
        port: endpoint.port(),
        memory,
    })
}
struct Encoded {
    path: Arc<dyn PacketPathCarrier>,
    source: UdpDatagramSource,
    target: Address,
    port: u16,
    memory: CodecMemory,
}
#[cfg(test)]
#[path = "encoded/tests/library.rs"]
mod library;
#[cfg(test)]
use library::wrap as wrap_library;
mod receive;
#[async_trait::async_trait]
impl PacketPathCarrier for Encoded {
    fn logical_registration(
        &self,
    ) -> Option<crate::component::datagram_retirement::DatagramRegistration> {
        self.path.logical_registration()
    }

    fn prepare_codec_send(
        &self,
        lengths: &[usize],
    ) -> Result<
        zero_platform_tokio::contract::component::PacketDriverSendBuffers,
        zero_platform_tokio::contract::ChannelError,
    > {
        self.path.prepare_codec_send(lengths)
    }
    #[cfg(any(feature = "peer-raw-ip-runtime", test))]
    fn codec_transmit_count(
        &self,
        target: &Address,
        port: u16,
        payload_len: usize,
    ) -> Result<usize, EngineError> {
        let own = self
            .source
            .codec
            .encode_buffer_layout(target, port, payload_len)
            .map_err(codec_error)?;
        let upper = *own.iter().max().expect("concrete nonempty inline layout");
        own.len()
            .checked_add(
                self.path
                    .codec_transmit_count(&self.target, self.port, upper)?,
            )
            .ok_or_else(layout_error)
    }
    #[cfg(feature = "peer-raw-ip-runtime")]
    fn prepare_codec_lengths(
        &self,
        count: usize,
    ) -> Result<zero_platform_tokio::contract::component::PacketMetadataVec<usize>, EngineError>
    {
        self.path.prepare_codec_lengths(count)
    }
    #[cfg(any(feature = "peer-raw-ip-runtime", test))]
    fn codec_transmit_lengths(
        &self,
        target: &Address,
        port: u16,
        payload_len: usize,
        lengths: &mut [usize],
    ) -> Result<(), EngineError> {
        let own = self
            .source
            .codec
            .encode_buffer_layout(target, port, payload_len)
            .map_err(codec_error)?;
        // All existing built-ins use one final buffer or an intermediate and
        // final pair; this finite table is not an allocator/plugin callback.
        if own.is_empty() || own.len() > 2 {
            return Err(layout_error());
        }
        let upper = *own.iter().max().expect("one concrete output");
        if own.len() > lengths.len() {
            return Err(layout_error());
        }
        let (local, remaining) = lengths.split_at_mut(own.len());
        local.copy_from_slice(&own);
        self.path
            .codec_transmit_lengths(&self.target, self.port, upper, remaining)
    }
    async fn send_codec_buffered(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
        buffers: &mut [zero_traits::PacketBuffer],
        copy: &mut zero_platform_tokio::contract::component::PacketDriverSendCopy,
    ) -> Result<(), EngineError> {
        let own = self
            .source
            .codec
            .encode_buffer_layout(target, port, payload.len())
            .map_err(codec_error)?;
        if own.is_empty() || own.len() > 2 || own.len() > buffers.len() {
            return Err(layout_error());
        }
        let (local, remaining) = buffers.split_at_mut(own.len());
        // The complete chain was admitted atomically before this first call.
        // Pure recomputation with actual output lengths can only narrow bounds.
        if local
            .iter()
            .zip(own.iter())
            .any(|(buffer, needed)| buffer.len() < *needed)
        {
            return Err(layout_error());
        }
        let encoded = self
            .source
            .codec
            .encode_buffered(target, port, payload, local)
            .map_err(codec_error)?;
        let wire = local
            .get(encoded.buffer)
            .and_then(|buffer| buffer.get(encoded.range))
            .ok_or_else(layout_error)?;
        // All intermediates and originals remain alive through the actual
        // underlying send, even when its ciphertext only references one slice.
        self.path
            .send_codec_buffered(&self.target, self.port, wire, remaining, copy)
            .await
    }
    #[cfg(feature = "peer-raw-ip-runtime")]
    fn request_driver_retirement(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.path.request_driver_retirement(driver)
    }
    #[cfg(feature = "peer-raw-ip-runtime")]
    fn confirm_driver_handoff(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.path.confirm_driver_handoff(driver)
    }
    #[cfg(feature = "peer-raw-ip-runtime")]
    async fn retire_driver_handoff(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
        deadline: tokio::time::Instant,
    ) -> Result<
        zero_platform_tokio::contract::lifecycle::CloseReport,
        zero_platform_tokio::contract::ChannelError,
    > {
        self.path.retire_driver_handoff(driver, deadline).await
    }

    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        match &self.memory {
            #[cfg(feature = "peer-raw-ip-runtime")]
            CodecMemory::Source(_) => {
                let count = self.codec_transmit_count(target, port, payload.len())?;
                let mut lengths = self.prepare_codec_lengths(count)?;
                self.codec_transmit_lengths(target, port, payload.len(), &mut lengths)?;
                let buffers = self
                    .prepare_codec_send(&lengths)
                    .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
                drop(lengths);
                let (mut buffers, mut copy) = buffers.into_batch_parts();
                self.send_codec_buffered(target, port, payload, &mut buffers, &mut copy)
                    .await
            }
            CodecMemory::Business(memory) => {
                let wire = memory.encode(self.source.codec.as_ref(), target, port, payload)?;
                self.path.send_to(&self.target, self.port, &wire).await
            }
            #[cfg(test)]
            CodecMemory::Library => self.send_library(target, port, payload).await,
        }
    }
    async fn recv_from(&self, output: &mut [u8]) -> Result<usize, EngineError> {
        self.recv_decoded(output).await.map(|(size, _)| size)
    }

    async fn recv_from_with_source(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        self.recv_decoded(output).await
    }
}

fn layout_error() -> EngineError {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "same codec returned an invalid admitted workspace layout",
    )
    .into()
}
fn codec_error(source: zero_traits::DatagramCodecBufferError<zero_core::Error>) -> EngineError {
    use zero_platform_tokio::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};
    let reason = match &source {
        zero_traits::DatagramCodecBufferError::Storage(
            zero_traits::PacketDeriveError::Capacity
            | zero_traits::PacketDeriveError::Busy
            | zero_traits::PacketDeriveError::Overflow,
        ) => Reason::Capacity,
        zero_traits::DatagramCodecBufferError::Storage(zero_traits::PacketDeriveError::Revoked) => {
            Reason::Revoked
        }
        zero_traits::DatagramCodecBufferError::Storage(_) => Reason::InvalidConfig,
        zero_traits::DatagramCodecBufferError::Protocol(_) => Reason::Protocol,
    };
    let mut failure = ChannelError::new(Stage::Send, reason, Impact::Packet).with_source(source);
    failure.delivery = DeliveryState::NotSubmitted;
    std::io::Error::other(failure).into()
}
