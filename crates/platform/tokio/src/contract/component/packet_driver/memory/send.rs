//! Same pre-native copy/target slots plus one atomic concrete codec chain.
use super::*;
/// Concrete fixed buffers for this actual Source's one serialized send.
/// It cannot create a Root, change policy, or authorize native IO.
pub struct PacketDriverSendBuffers {
    buffers: Vec<zero_traits::PacketBuffer>,
    copy: PacketDriverSendCopy,
}
pub struct PacketDriverSendCopy {
    #[cfg(feature = "native-packet-origin")]
    record: RecordRef,
    #[cfg(feature = "native-packet-origin")]
    buffer: Option<zero_tun::packet_memory::NativeReadBuffer>,
    #[cfg(feature = "native-packet-origin")]
    target: Option<zero_tun::packet_memory::NativeReadBuffer>,
}
impl PacketDriverSendBuffers {
    pub fn into_parts(self) -> (Vec<zero_traits::PacketBuffer>, PacketDriverSendCopy) {
        (self.buffers, self.copy)
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
            let payload =
                zero_traits::PacketBuffer::from_owner(DatagramCopyStorage { payload, metadata });
            // Datagram declares address before payload. The owned String is
            // dropped before its same backing-credit custody in payload.
            Ok(zero_core::packet::Datagram {
                association,
                address: zero_core::request::Destination::new(address, port),
                payload,
            })
        }
    }

    fn copy_packet(&mut self, bytes: &[u8]) -> Result<zero_traits::PacketBuffer, ChannelError> {
        #[cfg(not(feature = "native-packet-origin"))]
        {
            let _ = bytes;
            Err(failure(Reason::Unsupported))
        }
        #[cfg(feature = "native-packet-origin")]
        {
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
}
impl PacketDriverDatagramHandoff {
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
            let grant = self
                .installed
                .memory
                .get()
                .map(|memory| &memory.grant)
                .ok_or_else(|| failure(Reason::InvalidConfig))?;
            let buffer = grant.try_buffer().map_err(memory_error)?;
            let target = self
                .installed
                .memory
                .get()
                .map(|memory| &memory.target)
                .ok_or_else(|| failure(Reason::InvalidConfig))?
                .try_buffer()
                .map_err(memory_error)?;
            let buffers = grant
                .memory_view()
                .map_err(memory_error)?
                .transmit_buffers(lengths)
                .map_err(memory_error)?;
            Ok(PacketDriverSendBuffers {
                buffers,
                copy: PacketDriverSendCopy {
                    record: self.record.clone(),
                    buffer: Some(buffer),
                    target: Some(target),
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
    fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<zero_traits::PacketBuffer>, zero_traits::PacketDeriveError> {
        self.payload.try_derive_zeroed(lengths)
    }
}
