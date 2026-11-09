use super::*;
/// The actual allocation precedes its refund fields, including all errors.
/// Capacity growth prepays old+new backing before calling Vec's allocator.
pub(crate) struct WireBuffer {
    bytes: Vec<u8>,
    credit: Option<Credit>,
    header: Option<PacketMetadataCredit>,
    memory: Option<DnsWireMemory>,
}
impl WireBuffer {
    pub(crate) fn new(capacity: usize, memory: Option<&DnsWireMemory>) -> io::Result<Self> {
        let header = memory
            .map(|memory| memory.header::<response::Stored>())
            .transpose()?;
        let credit = memory
            .map(|memory| {
                memory.reserve(PacketMetadataLayout::array::<u8>(capacity).map_err(memory_error)?)
            })
            .transpose()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(io::Error::other)?;
        Ok(Self {
            bytes,
            credit,
            header,
            memory: memory.cloned(),
        })
    }
    pub(crate) fn zeroed(size: usize, memory: Option<&DnsWireMemory>) -> io::Result<Self> {
        let mut value = Self::new(size, memory)?;
        value.bytes.resize(size, 0);
        Ok(value)
    }
    pub(crate) fn copy(bytes: &[u8], memory: Option<&DnsWireMemory>) -> io::Result<Self> {
        let mut value = Self::new(bytes.len(), memory)?;
        value.bytes.extend_from_slice(bytes);
        Ok(value)
    }
    pub(crate) fn extend(&mut self, extra: &[u8]) -> io::Result<()> {
        let needed = self
            .bytes
            .len()
            .checked_add(extra.len())
            .ok_or_else(|| memory_error(PacketMemoryError::Overflow))?;
        if needed > self.bytes.capacity() {
            let additional = self
                .memory
                .as_ref()
                .map(|memory| {
                    memory.reserve(PacketMetadataLayout::array::<u8>(needed).map_err(memory_error)?)
                })
                .transpose()?;
            // Old storage and its ticket remain live during allocator growth.
            self.bytes
                .try_reserve_exact(needed - self.bytes.len())
                .map_err(io::Error::other)?;
            let retired = std::mem::replace(&mut self.credit, additional);
            drop(retired);
        }
        self.bytes.extend_from_slice(extra);
        Ok(())
    }
    pub(crate) fn extend_zeroed(&mut self, length: usize) -> io::Result<()> {
        let needed = self
            .bytes
            .len()
            .checked_add(length)
            .ok_or_else(|| memory_error(PacketMemoryError::Overflow))?;
        if needed > self.bytes.capacity() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "premeasured DNS padding exceeds its original workspace",
            ));
        }
        self.bytes.resize(needed, 0);
        Ok(())
    }
    /// Internal DNS constructors receive only their already measured output
    /// region. They append a proven finite wire size without reallocating.
    pub(super) fn build(&mut self, fill: impl FnOnce(&mut Vec<u8>)) {
        let capacity = self.bytes.capacity();
        fill(&mut self.bytes);
        assert_eq!(
            self.bytes.capacity(),
            capacity,
            "premeasured DNS output capacity"
        );
    }
    pub(crate) fn truncate(&mut self, size: usize) {
        self.bytes.truncate(size);
    }
    pub(crate) fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
    pub(crate) fn freeze(self) -> io::Result<DnsWireResponse> {
        let Self {
            bytes,
            credit,
            header,
            memory,
        } = self;
        drop(memory);
        let value = response::Stored {
            bytes,
            _credit: credit,
        };
        match header {
            Some(header) => {
                zero_traits::packet_metadata::PacketMetadataArc::from_prepaid(value, header)
                    .map(response::Backing::Host)
                    .map(DnsWireResponse)
                    .map_err(|(error, value, header)| {
                        drop(value);
                        drop(header);
                        memory_error(error)
                    })
            }
            None => Ok(DnsWireResponse(response::Backing::Library(Arc::new(value)))),
        }
    }
}
impl std::ops::Deref for WireBuffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
