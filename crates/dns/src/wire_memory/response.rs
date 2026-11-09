use super::*;
use std::ops::Deref;
use zero_traits::packet_metadata::PacketMetadataArc;
pub(super) struct Stored {
    pub(super) bytes: Vec<u8>,
    pub(super) _credit: Option<Credit>,
}
#[derive(Clone)]
pub(super) enum Backing {
    Host(PacketMetadataArc<Stored>),
    Library(Arc<Stored>),
}
/// Immutable wire bytes keep the actual backing/header credit until their
/// final shared owner is destroyed, even after a Query's true Scope ACK.
/// Cloning shares that original; transaction-ID/TTL changes prepay a new copy.
#[derive(Clone)]
pub struct DnsWireResponse(pub(super) Backing);
impl DnsWireResponse {
    pub(crate) fn library(bytes: Vec<u8>) -> Self {
        Self(Backing::Library(Arc::new(Stored {
            bytes,
            _credit: None,
        })))
    }
    pub(crate) fn copy(bytes: &[u8], memory: Option<&DnsWireMemory>) -> io::Result<Self> {
        WireBuffer::copy(bytes, memory)?.freeze()
    }
    pub(crate) fn mutable_copy(&self, memory: Option<&DnsWireMemory>) -> io::Result<WireBuffer> {
        WireBuffer::copy(self, memory)
    }
    pub fn as_slice(&self) -> &[u8] {
        self
    }
    pub fn allocation_bytes(&self) -> usize {
        self.stored().bytes.capacity()
    }
    fn stored(&self) -> &Stored {
        match &self.0 {
            Backing::Host(value) => value,
            Backing::Library(value) => value,
        }
    }
}
impl Deref for DnsWireResponse {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.stored().bytes
    }
}
impl AsRef<[u8]> for DnsWireResponse {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
impl std::fmt::Debug for DnsWireResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsWireResponse")
            .field("length", &self.len())
            .field("capacity", &self.allocation_bytes())
            .finish_non_exhaustive()
    }
}
impl PartialEq for DnsWireResponse {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}
impl Eq for DnsWireResponse {}
impl PartialEq<Vec<u8>> for DnsWireResponse {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_ref() == other.as_slice()
    }
}
impl PartialEq<DnsWireResponse> for Vec<u8> {
    fn eq(&self, other: &DnsWireResponse) -> bool {
        self.as_slice() == other.as_ref()
    }
}
impl std::hash::Hash for DnsWireResponse {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_ref(), state)
    }
}
