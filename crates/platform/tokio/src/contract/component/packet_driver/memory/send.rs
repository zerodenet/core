//! Same pre-native copy/target slots plus one atomic concrete codec chain.
use super::*;
/// Concrete fixed buffers for this actual Source's one serialized send.
/// It cannot create a Root, change policy, or authorize native IO.
pub struct PacketDriverSendBuffers {
    buffers: zero_traits::PacketBatch,
    copy: PacketDriverSendCopy,
}
pub struct PacketDriverSendCopy {
    #[cfg(feature = "native-packet-origin")]
    record: RecordRef,
    #[cfg(feature = "native-packet-origin")]
    buffer: Option<zero_tun::packet_memory::NativeReadBuffer>,
    #[cfg(feature = "native-packet-origin")]
    target: Option<zero_tun::packet_memory::NativeReadBuffer>,
    #[cfg(feature = "native-packet-origin")]
    header: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
}
impl PacketDriverSendBuffers {
    pub fn into_batch_parts(self) -> (zero_traits::PacketBatch, PacketDriverSendCopy) {
        (self.buffers, self.copy)
    }
    pub fn into_parts(self) -> (Vec<zero_traits::PacketBuffer>, PacketDriverSendCopy) {
        (self.buffers.into_iter().collect(), self.copy)
    }
}
impl PacketDriverSendCopy {
    pub fn copy_datagram(
        &mut self,
        association: AssociationId,
        target: &zero_core::Address,
        port: u16,
        bytes: &[u8],
    ) -> Result<zero_core::packet::Datagram, ChannelError> {
        #[cfg(not(feature = "native-packet-origin"))]
        {
            let _ = (association, target, port, bytes);
            Err(failure(Reason::Unsupported))
        }
        #[cfg(feature = "native-packet-origin")]
        {
            if self.header.is_none() {
                return Err(failure(Reason::Protocol));
            }
            let mut metadata = None;
            let address = match target {
                zero_core::Address::Domain(name) => {
                    let target = self
                        .target
                        .as_ref()
                        .ok_or_else(|| failure(Reason::Protocol))?;
                    if name.len() > 255 || name.len() > target.as_bytes().len() {
                        return Err(failure(Reason::Capacity));
                    }
                    let target = self.target.take().expect("same checked target backing");
                    let mut owner = target.into_packet(255).map_err(|original| {
                        drop(original);
                        failure(Reason::Internal)
                    })?;
                    let mut allocation = owner
                        .take_vec_allocation()
                        .ok_or_else(|| failure(Reason::Internal))?;
                    allocation[..name.len()].copy_from_slice(name.as_bytes());
                    allocation.truncate(name.len());
                    let name = String::from_utf8(allocation)
                        .map_err(|source| failure(Reason::Protocol).with_source(source))?;
                    metadata = Some(owner);
                    zero_core::Address::Domain(name)
                }
                _ => target.clone(),
            };
            let payload = self.copy_packet(bytes)?;
            let header = self.header.take().expect("same unconsumed copy header");
            let payload = match zero_traits::PacketBuffer::from_prepaid_owner(
                DatagramCopyStorage { payload, metadata },
                header,
            ) {
                Ok(payload) => payload,
                Err((source, owner, header)) => {
                    drop(address); // moved Domain storage remains paid until this drop
                    drop(owner);
                    drop(header);
                    return Err(memory_error(source));
                }
            };
            // Datagram declares address before payload. The owned String is
            // dropped before its same backing-credit custody in payload.
            Ok(zero_core::packet::Datagram {
                association,
                address: zero_core::request::Destination::new(address, port),
                payload,
            })
        }
    }

    #[cfg(feature = "native-packet-origin")]
    fn copy_packet(&mut self, bytes: &[u8]) -> Result<zero_traits::PacketBuffer, ChannelError> {
        if self.record.call.cancel.is_cancelled() || self.record.original.is_retired() {
            return Err(failure(Reason::Revoked));
        }
        let buffer = self
            .buffer
            .as_mut()
            .ok_or_else(|| failure(Reason::Protocol))?;
        if bytes.len() > buffer.as_bytes().len() {
            return Err(failure(Reason::Capacity));
        }
        buffer.as_mut_bytes()[..bytes.len()].copy_from_slice(bytes);
        self.buffer
            .take()
            .expect("same checked unique copy")
            .into_packet(bytes.len())
            .map_err(|original| {
                drop(original);
                failure(Reason::Internal)
            })
    }
}
impl PacketDriverDatagramHandoff {
    /// Pure dimensions for the actual installed sender, paid on its same
    /// Source metadata Root. This grants no additional send/IO permission.
    pub fn prepare_codec_lengths(
        &self,
        count: usize,
    ) -> Result<crate::contract::component::PacketMetadataVec<usize>, ChannelError> {
        #[cfg(not(feature = "native-packet-origin"))]
        {
            let _ = count;
            Err(failure(Reason::Unsupported))
        }
        #[cfg(feature = "native-packet-origin")]
        {
            let original = self.send_original()?;
            let memory = original
                .grant
                .memory_view()
                .map_err(memory_error)?
                .driver_metadata_view()
                .map_err(memory_error)?;
            prepare_lengths(&memory, count)
        }
    }
    #[cfg(feature = "native-packet-origin")]
    fn send_original(&self) -> Result<&WireCopy, ChannelError> {
        if self.record.call.cancel.is_cancelled()
            || self.record.original.is_retired()
            || self
                .installed
                .receiver
                .get()
                .is_none_or(TaskJoinWitness::is_joined)
        {
            return Err(failure(Reason::Revoked));
        }
        self.installed
            .memory
            .get()
            .ok_or_else(|| failure(Reason::InvalidConfig))
    }
    /// Acquire the actual final copy slot AND every codec-layer allocation
    /// before the first codec can advance nonce/RNG/replay state. A slot still
    /// held by a previous queued send is NotSubmitted, never a late failure
    /// discovered after an outer codec has already changed security state.
    pub fn prepare_send(&self, lengths: &[usize]) -> Result<PacketDriverSendBuffers, ChannelError> {
        #[cfg(not(feature = "native-packet-origin"))]
        {
            let _ = lengths;
            Err(failure(Reason::Unsupported))
        }
        #[cfg(feature = "native-packet-origin")]
        {
            let copy = self.send_original()?;
            let grant = &copy.grant;
            let memory = grant.memory_view().map_err(memory_error)?;
            let header = memory
                .driver_metadata_view()
                .map_err(memory_error)?
                .reserve(zero_traits::packet_metadata::PacketMetadataLayout::boxed::<
                    DatagramCopyStorage,
                >())
                .map_err(memory_error)?;
            let buffer = grant.try_buffer().map_err(memory_error)?;
            let target = copy.target.try_buffer().map_err(memory_error)?;
            let buffers = grant
                .memory_view()
                .map_err(memory_error)?
                .transmit_batch(lengths)
                .map_err(memory_error)?;
            Ok(PacketDriverSendBuffers {
                buffers,
                copy: PacketDriverSendCopy {
                    record: self.record.clone(),
                    buffer: Some(buffer),
                    target: Some(target),
                    header: Some(header),
                },
            })
        }
    }
}

#[cfg(feature = "native-packet-origin")]
struct DatagramCopyStorage {
    payload: zero_traits::PacketBuffer,
    metadata: Option<zero_traits::PacketBuffer>,
}
#[cfg(feature = "native-packet-origin")]
impl AsRef<[u8]> for DatagramCopyStorage {
    fn as_ref(&self) -> &[u8] {
        self.payload.as_ref()
    }
}
#[cfg(feature = "native-packet-origin")]
impl AsMut<[u8]> for DatagramCopyStorage {
    fn as_mut(&mut self) -> &mut [u8] {
        self.payload.as_mut()
    }
}
#[cfg(feature = "native-packet-origin")]
impl zero_traits::PacketStorage for DatagramCopyStorage {
    fn allocation_bytes(&self) -> usize {
        self.payload.allocation_bytes()
            + self
                .metadata
                .as_ref()
                .map_or(0, zero_traits::PacketBuffer::allocation_bytes)
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        self.payload.take_vec_allocation()
    }
    fn prepare_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), zero_traits::PacketDeriveError> {
        self.payload.prepare_transmit(frames, bytes)
    }
    fn try_derive_owned(
        &self,
        lengths: &[usize],
    ) -> Result<zero_traits::PacketBatch, zero_traits::PacketDeriveError> {
        self.payload.try_derive_owned(lengths)
    }
    fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<zero_traits::PacketBuffer>, zero_traits::PacketDeriveError> {
        self.payload.try_derive_zeroed(lengths)
    }
}

#[cfg(feature = "native-packet-origin")]
fn prepare_lengths(
    memory: &zero_tun::packet_memory::PacketDriverMetadataView,
    count: usize,
) -> Result<zero_tun::packet_memory::PacketMetadataVec<usize>, ChannelError> {
    let mut lengths = zero_tun::packet_memory::PacketMetadataVec::default();
    for _ in 0..count {
        // The full actual chain count is already checked. Geometric growth
        // uses the existing vector's old+new admission and deallocation order.
        lengths
            .try_push_with_metadata(0, memory, count)
            .map_err(|(source, _)| memory_error(source))?;
    }
    Ok(lengths)
}
#[cfg(all(test, feature = "native-packet-origin"))]
#[path = "send/tests.rs"]
mod tests;
