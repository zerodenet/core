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

pub(crate) fn wrap(
    path: Arc<dyn PacketPathCarrier>,
    source: UdpDatagramSource,
) -> Arc<dyn PacketPathCarrier> {
    wrap_with_memory(path, source, None, None)
}
pub(crate) fn wrap_with_memory(
    path: Arc<dyn PacketPathCarrier>,
    source: UdpDatagramSource,
    memory: Option<crate::protocol_registry::packet_memory::PacketCodecReceiveMemory>,
    business: Option<crate::protocol_registry::packet_memory::business::BusinessMemory>,
) -> Arc<dyn PacketPathCarrier> {
    let endpoint = source.descriptor().endpoint();
    Arc::new(Encoded {
        path,
        source,
        target: endpoint.target(),
        port: endpoint.port(),
        memory,
        business,
    })
}
struct Encoded {
    path: Arc<dyn PacketPathCarrier>,
    source: UdpDatagramSource,
    target: Address,
    port: u16,
    memory: Option<crate::protocol_registry::packet_memory::PacketCodecReceiveMemory>,
    business: Option<crate::protocol_registry::packet_memory::business::BusinessMemory>,
}
impl Encoded {
    async fn recv_decoded(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        if self.has_driver_handoff() {
            let memory = self.memory.as_ref().ok_or_else(layout_error)?;
            return memory
                .receive(self.path.as_ref(), self.source.codec.as_ref(), output)
                .await;
        }
        if let Some(memory) = &self.business {
            let mut wire = memory.buffer(65536)?;
            loop {
                let length = self.path.recv_from(&mut wire).await?;
                let decoded = match memory.decode(self.source.codec.as_ref(), &wire[..length]) {
                    Err(source)
                        if crate::protocol_registry::packet_memory::business::is_pressure(
                            &source,
                        ) =>
                    {
                        tracing::warn!(%source,"original Host cannot admit this new codec response");
                        tokio::task::consume_budget().await;
                        continue;
                    }
                    result => result?,
                };
                let Some((target, port, payload)) = decoded else {
                    tokio::task::consume_budget().await;
                    continue;
                };
                if payload.len() > output.len() {
                    drop(target);
                    drop(payload);
                    return Err(layout_error());
                }
                output[..payload.len()].copy_from_slice(&payload);
                let result = (payload.len(), packet_path_source(&target, port));
                drop(target);
                drop(payload);
                return Ok(result);
            }
        }
        // Remaining compatibility consumers retain their previous ownership;
        // absence of a Source grant does not identify a library caller.
        let mut wire = vec![0; 65536];
        loop {
            let length = self.path.recv_from(&mut wire).await?;
            let Some((source, port, payload)) = self.source.codec.decode(&wire[..length]) else {
                tokio::task::consume_budget().await;
                continue;
            };
            if payload.len() > output.len() {
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "decoded relay datagram exceeds receive buffer",
                )));
            }
            output[..payload.len()].copy_from_slice(&payload);
            return Ok((payload.len(), packet_path_source(&source, port)));
        }
    }
}
#[async_trait::async_trait]
impl PacketPathCarrier for Encoded {
    fn logical_registration(
        &self,
    ) -> Option<crate::component::datagram_retirement::DatagramRegistration> {
        self.path.logical_registration()
    }

    fn has_driver_handoff(&self) -> bool {
        self.path.has_driver_handoff()
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
    fn codec_transmit_lengths(
        &self,
        target: &Address,
        port: u16,
        payload_len: usize,
        lengths: &mut Vec<usize>,
    ) -> Result<(), EngineError> {
        let own = self
            .source
            .codec
            .encode_buffer_lengths(target, port, payload_len)
            .map_err(codec_error)?;
        // All existing built-ins use one final buffer or an intermediate and
        // final pair; this finite table is not an allocator/plugin callback.
        if own.is_empty() || own.len() > 2 {
            return Err(layout_error());
        }
        let upper = *own.iter().max().expect("one concrete output");
        lengths.extend_from_slice(&own);
        self.path
            .codec_transmit_lengths(&self.target, self.port, upper, lengths)
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
            .encode_buffer_lengths(target, port, payload.len())
            .map_err(codec_error)?;
        if own.is_empty() || own.len() > 2 || own.len() > buffers.len() {
            return Err(layout_error());
        }
        let (local, remaining) = buffers.split_at_mut(own.len());
        // The complete chain was admitted atomically before this first call.
        // Pure recomputation with actual output lengths can only narrow bounds.
        if local
            .iter()
            .zip(&own)
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
    fn request_driver_retirement(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.path.request_driver_retirement(driver)
    }
    fn confirm_driver_handoff(
        &self,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.path.confirm_driver_handoff(driver)
    }
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
    fn native_owner(
        &self,
    ) -> Option<Arc<crate::runtime::udp_dispatch::packet_owner::PacketCarrierOwnerSlot>> {
        self.path.native_owner()
    }

    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        if self.has_driver_handoff() {
            let mut lengths = Vec::new();
            self.codec_transmit_lengths(target, port, payload.len(), &mut lengths)?;
            let buffers = self
                .prepare_codec_send(&lengths)
                .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
            let (mut buffers, mut copy) = buffers.into_batch_parts();
            return self
                .send_codec_buffered(target, port, payload, &mut buffers, &mut copy)
                .await;
        }
        if let Some(memory) = &self.business {
            let wire = memory.encode(self.source.codec.as_ref(), target, port, payload)?;
            return self.path.send_to(&self.target, self.port, &wire).await;
        }
        // Remaining compatibility consumers have not yet migrated their book.
        let wire = self
            .source
            .codec
            .encode(target, port, payload)
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
        let endpoint = self.source.descriptor().endpoint();
        self.path
            .send_to(&endpoint.target(), endpoint.port(), &wire)
            .await
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
