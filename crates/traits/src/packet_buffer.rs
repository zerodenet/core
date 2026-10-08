//! Exclusive packet ownership across protocol and runtime boundaries.
use alloc::{boxed::Box, vec::Vec};
use core::{
    fmt,
    ops::{Deref, DerefMut},
};

/// Storage remains owned until the consumer finishes or drops the packet.
/// Protocols may retain a pool return guard inside the owner. Mutation must
/// expose the same initialized packet bytes, without aliases held by producers.
pub trait PacketStorage: AsRef<[u8]> + AsMut<[u8]> + Send + Sync + 'static {
    /// Owners may transfer an exclusive allocation to a Vec. The default
    /// copies, preserving any original pool/shared-owner lifetime rules.
    fn into_vec(self: Box<Self>) -> Vec<u8> {
        self.as_ref().as_ref().to_vec()
    }
}
impl PacketStorage for Vec<u8> {
    fn into_vec(self: Box<Self>) -> Vec<u8> {
        *self
    }
}

/// A movable, exclusively mutable packet without implicit cloning. Vec and
/// BytesMut move directly without an owner allocation or a storage clone;
/// existing backing-allocation lifetime metadata moves with the bytes.
pub struct PacketBuffer(Storage);
enum Storage {
    Vector(Vec<u8>),
    Bytes(bytes::BytesMut),
    Owner(Box<dyn PacketStorage>),
}
impl PacketBuffer {
    pub fn from_owner(owner: impl PacketStorage) -> Self {
        Self(Storage::Owner(Box::new(owner)))
    }
    /// Preserve an existing Vec, or copy at an explicitly Vec-only boundary.
    pub fn into_vec(self) -> Vec<u8> {
        match self.0 {
            Storage::Vector(bytes) => bytes,
            Storage::Bytes(bytes) => bytes.into(),
            Storage::Owner(owner) => owner.into_vec(),
        }
    }
}
impl From<Vec<u8>> for PacketBuffer {
    fn from(bytes: Vec<u8>) -> Self {
        Self(Storage::Vector(bytes))
    }
}
impl From<bytes::BytesMut> for PacketBuffer {
    fn from(bytes: bytes::BytesMut) -> Self {
        Self(Storage::Bytes(bytes))
    }
}
impl Default for PacketBuffer {
    fn default() -> Self {
        Vec::new().into()
    }
}
impl AsRef<[u8]> for PacketBuffer {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Storage::Vector(bytes) => bytes,
            Storage::Bytes(bytes) => bytes,
            Storage::Owner(owner) => owner.as_ref().as_ref(),
        }
    }
}
impl AsMut<[u8]> for PacketBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        match &mut self.0 {
            Storage::Vector(bytes) => bytes,
            Storage::Bytes(bytes) => bytes,
            Storage::Owner(owner) => owner.as_mut().as_mut(),
        }
    }
}
impl Deref for PacketBuffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}
impl DerefMut for PacketBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}
impl fmt::Debug for PacketBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PacketBuffer")
            .field("len", &self.len())
            .finish()
    }
}
impl PartialEq for PacketBuffer {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}
impl Eq for PacketBuffer {}
impl PartialEq<Vec<u8>> for PacketBuffer {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_ref() == other.as_slice()
    }
}
