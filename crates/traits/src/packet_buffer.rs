//! Exclusive packet ownership across protocol and runtime boundaries.
use alloc::{boxed::Box, vec::Vec};
use core::{
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
};

/// Storage remains owned until the consumer finishes or drops the packet.
/// Protocols may retain a pool return guard inside the owner. Mutation must
/// expose the same initialized packet bytes, without aliases held by producers.
/// A concrete packet transformation can only claim its original owner's
/// pre-reserved storage. This API grants no native IO, task or close authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketDeriveError {
    Unreserved,
    Capacity,
    Busy,
    Revoked,
    Overflow,
    LayoutMismatch,
    AlreadyDerived,
}
impl fmt::Display for PacketDeriveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "packet derivation {self:?}")
    }
}
impl core::error::Error for PacketDeriveError {}
pub type PacketDerivation<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<PacketBuffer>, PacketDeriveError>> + Send + 'a>>;

pub trait PacketStorage: AsRef<[u8]> + AsMut<[u8]> + Send + Sync + 'static {
    /// Claim only this original's pre-reserved derived buffer lengths. The
    /// default refuses: unaccounted library storage cannot qualify production
    /// transformation by silently allocating a second set of frames.
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> PacketDerivation<'a> {
        Box::pin(async move { self.try_derive_zeroed(lengths) })
    }
    /// Split only already admitted memory. This performs no IO or waiting and
    /// creates no source/task permission. Poll-based original owners use this
    /// before enqueueing, retaining their exact original on any error.
    fn try_derive_zeroed(
        &self,
        _lengths: &[usize],
    ) -> Result<Vec<PacketBuffer>, PacketDeriveError> {
        Err(PacketDeriveError::Unreserved)
    }
    /// Same already-admitted transform, including the actual returned array.
    /// Existing independent library Vec APIs keep their explicit ownership.
    fn try_derive_owned(
        &self,
        _lengths: &[usize],
    ) -> Result<crate::PacketBatch, PacketDeriveError> {
        Err(PacketDeriveError::Unreserved)
    }
    /// Admit this original's complete transmit transform before enqueueing it.
    /// Concrete owned storage reserves every returned allocation in the same
    /// Kernel root and preserves the original on error. No IO/task permission
    /// or closure evidence is created. Plain library storage refuses.
    fn prepare_transmit(&mut self, _frames: usize, _bytes: usize) -> Result<(), PacketDeriveError> {
        Err(PacketDeriveError::Unreserved)
    }
    /// Move the unique Vec allocation while retaining this exact storage owner
    /// as its custody. The consumer must keep this owner alive until the moved
    /// bytes actually finish all uses. Unknown/shared/pool storage refuses.
    /// Taking bytes does not refund memory or prove IO/task/native closure.
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        None
    }
    /// Bytes retained by this owned allocation, including initialized spare
    /// capacity covered by an original IO/workspace reservation. This is memory
    /// accounting metadata, never native or task authority.
    fn allocation_bytes(&self) -> usize {
        self.as_ref().len()
    }
    /// Owners may transfer an exclusive allocation to a Vec. The default
    /// copies, preserving any original pool/shared-owner lifetime rules.
    fn into_vec(self: Box<Self>) -> Vec<u8> {
        self.as_ref().as_ref().to_vec()
    }
}
impl PacketStorage for Vec<u8> {
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        Some(core::mem::take(self))
    }
    fn allocation_bytes(&self) -> usize {
        self.capacity()
    }
    fn into_vec(self: Box<Self>) -> Vec<u8> {
        *self
    }
}

/// A movable, exclusively mutable packet without implicit cloning. Vec and
/// BytesMut move directly without an owner allocation or a storage clone;
/// existing backing-allocation lifetime metadata moves with the bytes.
pub struct PacketBuffer {
    storage: Storage,
    _metadata: Option<Metadata>,
}
enum Metadata {
    Credit {
        _credit: crate::packet_metadata::PacketMetadataCredit,
    },
    Header {
        _header: crate::packet_metadata::PacketMetadataHeaderLease,
    },
}
enum Storage {
    Vector(Vec<u8>),
    Bytes(bytes::BytesMut),
    Owner(Box<dyn PacketStorage>),
}
impl PacketBuffer {
    pub fn from_owner(owner: impl PacketStorage) -> Self {
        Self {
            storage: Storage::Owner(Box::new(owner)),
            _metadata: None,
        }
    }
    /// Metadata is prepaid before allocating this concrete storage Box. The
    /// credit field follows Storage, so even unwinding deallocates the actual
    /// Box before its bytes/header units return. Plain library constructors
    /// retain their original explicit unmetered responsibility.
    pub fn from_prepaid_owner<T: PacketStorage>(
        owner: T,
        credit: crate::packet_metadata::PacketMetadataCredit,
    ) -> Result<
        Self,
        (
            crate::packet_metadata::PacketMemoryError,
            T,
            crate::packet_metadata::PacketMetadataCredit,
        ),
    > {
        let expected = crate::packet_metadata::PacketMetadataLayout::boxed::<T>();
        let actual = credit.layout();
        if actual.allocations() < expected.allocations() || actual.bytes() < expected.bytes() {
            return Err((
                crate::packet_metadata::PacketMemoryError::Capacity,
                owner,
                credit,
            ));
        }
        Ok(Self {
            storage: Storage::Owner(Box::new(owner)),
            _metadata: Some(Metadata::Credit { _credit: credit }),
        })
    }
    /// Consume only the original workspace's already-paid reusable header.
    /// The lease is released AFTER the actual erased Box deallocates.
    pub fn from_leased_owner<T: PacketStorage>(
        owner: T,
        header: crate::packet_metadata::PacketMetadataHeaderLease,
    ) -> Result<
        Self,
        (
            crate::packet_metadata::PacketMemoryError,
            T,
            crate::packet_metadata::PacketMetadataHeaderLease,
        ),
    > {
        let expected = crate::packet_metadata::PacketMetadataLayout::boxed::<T>();
        let actual = header.layout();
        if actual.allocations() < expected.allocations() || actual.bytes() < expected.bytes() {
            return Err((
                crate::packet_metadata::PacketMemoryError::Capacity,
                owner,
                header,
            ));
        }
        Ok(Self {
            storage: Storage::Owner(Box::new(owner)),
            _metadata: Some(Metadata::Header { _header: header }),
        })
    }
    pub fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> PacketDerivation<'a> {
        match &self.storage {
            Storage::Owner(owner) => owner.derive_zeroed(lengths),
            Storage::Vector(_) | Storage::Bytes(_) => {
                Box::pin(async { Err(PacketDeriveError::Unreserved) })
            }
        }
    }
    pub fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<PacketBuffer>, PacketDeriveError> {
        match &self.storage {
            Storage::Owner(owner) => owner.try_derive_zeroed(lengths),
            Storage::Vector(_) | Storage::Bytes(_) => Err(PacketDeriveError::Unreserved),
        }
    }
    pub fn try_derive_owned(
        &self,
        lengths: &[usize],
    ) -> Result<crate::PacketBatch, PacketDeriveError> {
        match &self.storage {
            Storage::Owner(owner) => owner.try_derive_owned(lengths),
            Storage::Vector(_) | Storage::Bytes(_) => Err(PacketDeriveError::Unreserved),
        }
    }
    pub fn prepare_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), PacketDeriveError> {
        match &mut self.storage {
            Storage::Owner(owner) => owner.prepare_transmit(frames, bytes),
            Storage::Vector(_) | Storage::Bytes(_) => Err(PacketDeriveError::Unreserved),
        }
    }
    /// The caller retains this same owner as the moved allocation's custody.
    pub fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        match &mut self.storage {
            Storage::Vector(bytes) => Some(core::mem::take(bytes)),
            Storage::Bytes(_) => None,
            Storage::Owner(owner) => owner.take_vec_allocation(),
        }
    }
    pub fn allocation_bytes(&self) -> usize {
        match &self.storage {
            Storage::Vector(bytes) => bytes.capacity(),
            Storage::Bytes(bytes) => bytes.capacity(),
            Storage::Owner(owner) => owner.allocation_bytes(),
        }
    }
    /// Preserve an existing Vec, or copy at an explicitly Vec-only boundary.
    pub fn into_vec(self) -> Vec<u8> {
        match self.storage {
            Storage::Vector(bytes) => bytes,
            Storage::Bytes(bytes) => bytes.into(),
            Storage::Owner(owner) => owner.into_vec(),
        }
    }
}
impl From<Vec<u8>> for PacketBuffer {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            storage: Storage::Vector(bytes),
            _metadata: None,
        }
    }
}
impl From<bytes::BytesMut> for PacketBuffer {
    fn from(bytes: bytes::BytesMut) -> Self {
        Self {
            storage: Storage::Bytes(bytes),
            _metadata: None,
        }
    }
}
impl Default for PacketBuffer {
    fn default() -> Self {
        Vec::new().into()
    }
}
impl AsRef<[u8]> for PacketBuffer {
    fn as_ref(&self) -> &[u8] {
        match &self.storage {
            Storage::Vector(bytes) => bytes,
            Storage::Bytes(bytes) => bytes,
            Storage::Owner(owner) => owner.as_ref().as_ref(),
        }
    }
}
impl AsMut<[u8]> for PacketBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        match &mut self.storage {
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
