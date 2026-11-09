//! The existing Host byte/metadata books pay this admitted caller's data.
//! These are storage owners; no Source grant or IO/Scope authority is issued.
use std::ops::Range;
use zero_core::{request::Destination, Address};
use zero_engine::EngineError;
use zero_platform_tokio::{
    contract::component::PacketDriverMetadataView,
    controlled_carrier::{CarrierBufferBudget, CarrierBufferCredit},
};
use zero_traits::packet_metadata::PacketMetadataLayout;
use zero_traits::{DatagramCodec, DatagramCodecOutput, PacketBuffer, PacketStorage};
#[derive(Clone)]
pub(crate) struct BusinessMemory {
    pub(crate) bytes: CarrierBufferBudget,
    pub(crate) metadata: PacketDriverMetadataView,
}
struct Backing {
    bytes: Vec<u8>,
    _credit: CarrierBufferCredit,
}
impl AsRef<[u8]> for Backing {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Backing {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl PacketStorage for Backing {
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        Some(std::mem::take(&mut self.bytes))
    }
    fn allocation_bytes(&self) -> usize {
        self._credit.bytes()
    }
}
impl Drop for Backing {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.bytes));
    }
}
struct Encoded {
    buffers: Vec<PacketBuffer>,
    buffer: usize,
    range: Range<usize>,
}
impl AsRef<[u8]> for Encoded {
    fn as_ref(&self) -> &[u8] {
        &self.buffers[self.buffer][self.range.clone()]
    }
}
impl AsMut<[u8]> for Encoded {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffers[self.buffer][self.range.clone()]
    }
}
impl PacketStorage for Encoded {}
impl BusinessMemory {
    pub(crate) fn from_services(
        services: &crate::protocol_registry::TcpExecutionServices,
    ) -> Result<Self, EngineError> {
        let host = services
            .current_source
            .upgrade()
            .ok_or_else(|| invalid("original Host is retired"))?;
        let (bytes, metadata) = host.packet_path_memory()?;
        Ok(Self { bytes, metadata })
    }
    pub(crate) fn same(&self, other: &Self) -> bool {
        self.bytes.same_budget(&other.bytes) && self.metadata.same_root(&other.metadata)
    }
    fn reserve_bytes(&self, size: usize) -> Result<CarrierBufferCredit, EngineError> {
        self.bytes.try_reserve(size).map_err(|source| {
            use zero_platform_tokio::contract::{
                ChannelError, DeliveryState, Impact, Reason, Stage,
            };
            let mut failure = ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Packet)
                .with_source(source);
            failure.delivery = DeliveryState::NotSubmitted;
            std::io::Error::other(failure).into()
        })
    }
    pub(crate) fn buffer(&self, size: usize) -> Result<PacketBuffer, EngineError> {
        let header = self
            .metadata
            .reserve(PacketMetadataLayout::boxed::<Backing>())
            .map_err(std::io::Error::other)?;
        let credit = self.reserve_bytes(size)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(std::io::Error::other)?;
        bytes.resize(size, 0);
        PacketBuffer::from_prepaid_owner(
            Backing {
                bytes,
                _credit: credit,
            },
            header,
        )
        .map_err(|(error, _, _)| std::io::Error::other(error).into())
    }
    pub(crate) fn datagram(
        &self,
        association: zero_core::component::AssociationId,
        target: &Address,
        port: u16,
        bytes: &[u8],
    ) -> Result<zero_core::packet::Datagram, EngineError> {
        let address_bytes = match target {
            Address::Domain(name) => name.len(),
            _ => 0,
        };
        let header = self
            .metadata
            .reserve(PacketMetadataLayout::boxed::<Backing>())
            .map_err(std::io::Error::other)?;
        let credit = self.reserve_bytes(
            bytes
                .len()
                .checked_add(address_bytes)
                .ok_or_else(|| invalid("packet address overflow"))?,
        )?;
        let payload = PacketBuffer::from_prepaid_owner(
            Backing {
                bytes: bytes.to_vec(),
                _credit: credit,
            },
            header,
        )
        .map_err(|(error, _, _)| std::io::Error::other(error))?;
        // Datagram drops its Address before its payload owner returns the same
        // ticket, including a cloned Domain allocation and pending writes.
        Ok(zero_core::packet::Datagram {
            association,
            address: Destination::new(target.clone(), port),
            payload,
        })
    }
    fn storage(
        &self,
        lengths: &[usize],
    ) -> Result<
        (
            Vec<PacketBuffer>,
            zero_traits::packet_metadata::PacketMetadataCredit,
        ),
        EngineError,
    > {
        if lengths.is_empty() || lengths.len() > 2 {
            return Err(invalid("actual codec workspace layout changed"));
        }
        let layout = PacketMetadataLayout::boxed::<Encoded>()
            .checked_add(
                PacketMetadataLayout::array::<PacketBuffer>(lengths.len())
                    .map_err(std::io::Error::other)?,
            )
            .map_err(std::io::Error::other)?;
        let header = self
            .metadata
            .reserve(layout)
            .map_err(std::io::Error::other)?;
        let mut buffers = Vec::new();
        buffers
            .try_reserve_exact(lengths.len())
            .map_err(std::io::Error::other)?;
        for &size in lengths {
            buffers.push(self.buffer(size)?);
        }
        Ok((buffers, header))
    }
    fn finish(
        &self,
        buffers: Vec<PacketBuffer>,
        output: DatagramCodecOutput,
        header: zero_traits::packet_metadata::PacketMetadataCredit,
    ) -> Result<PacketBuffer, EngineError> {
        if buffers
            .get(output.buffer)
            .and_then(|buffer| buffer.get(output.range.clone()))
            .is_none()
        {
            return Err(invalid("actual codec returned a foreign range"));
        }
        PacketBuffer::from_prepaid_owner(
            Encoded {
                buffers,
                buffer: output.buffer,
                range: output.range,
            },
            header,
        )
        .map_err(|(source, _, _)| std::io::Error::other(source).into())
    }
    pub(crate) fn encode(
        &self,
        codec: &dyn DatagramCodec<Address, Error = zero_core::Error>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<PacketBuffer, EngineError> {
        let _shape = self
            .metadata
            .reserve(PacketMetadataLayout::array::<usize>(2).map_err(std::io::Error::other)?)
            .map_err(std::io::Error::other)?;
        let lengths = codec
            .encode_buffer_lengths(target, port, payload.len())
            .map_err(std::io::Error::other)?;
        let (mut buffers, header) = self.storage(&lengths)?;
        let output = codec
            .encode_buffered(target, port, payload, &mut buffers)
            .map_err(std::io::Error::other)?;
        self.finish(buffers, output, header)
    }
    pub(crate) fn decode(
        &self,
        codec: &dyn DatagramCodec<Address, Error = zero_core::Error>,
        wire: &[u8],
    ) -> Result<Option<(Address, u16, PacketBuffer)>, EngineError> {
        let _shape = self
            .metadata
            .reserve(PacketMetadataLayout::array::<usize>(2).map_err(std::io::Error::other)?)
            .map_err(std::io::Error::other)?;
        let lengths = codec
            .decode_buffer_lengths(wire.len())
            .map_err(std::io::Error::other)?;
        let (mut buffers, header) = self.storage(&lengths)?;
        let Some((target, port, output)) = codec
            .decode_buffered(wire, &mut buffers)
            .map_err(std::io::Error::other)?
        else {
            return Ok(None);
        };
        // All original buffers, including the moved Domain backing's custody,
        // travel inside payload until the responder finishes its actual write.
        if buffers
            .get(output.buffer)
            .and_then(|buffer| buffer.get(output.range.clone()))
            .is_none()
        {
            drop(target);
            drop(buffers);
            drop(header);
            return Err(invalid("actual codec returned a foreign range"));
        }
        match PacketBuffer::from_prepaid_owner(
            Encoded {
                buffers,
                buffer: output.buffer,
                range: output.range,
            },
            header,
        ) {
            Ok(payload) => Ok(Some((target, port, payload))),
            Err((source, owner, header)) => {
                drop(target);
                drop(owner);
                drop(header);
                Err(std::io::Error::other(source).into())
            }
        }
    }
}
fn invalid(message: &'static str) -> EngineError {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message).into()
}

/// Only the concrete original memory refusal drops this new UDP frame; it does
/// not retire its existing socket/driver or evict a live queue/history row.
pub(crate) fn is_pressure(error: &EngineError) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(actual) = source {
        if actual
            .downcast_ref::<zero_traits::packet_metadata::PacketMemoryError>()
            .is_some_and(|error| {
                matches!(
                    error,
                    zero_traits::packet_metadata::PacketMemoryError::Capacity
                        | zero_traits::packet_metadata::PacketMemoryError::Busy
                        | zero_traits::packet_metadata::PacketMemoryError::Paused
                )
            })
        {
            return true;
        }
        if actual
            .downcast_ref::<zero_platform_tokio::contract::ChannelError>()
            .is_some_and(|error| error.reason == zero_platform_tokio::contract::Reason::Capacity)
        {
            return true;
        }
        source = actual.source();
    }
    false
}
