//! A concrete owned byte allocation and its same original custody. No native
//! resources, task authority, callback allocator or protocol driver are created.
use super::*;
impl Packet {
    /// Move a unique admitted Vec, retaining its exact original storage owner.
    /// A shared/pool-backed owner which cannot move its Vec is returned intact.
    pub fn from_owned_buffer(
        mut custody: zero_traits::PacketBuffer,
    ) -> Result<Self, zero_traits::PacketBuffer> {
        let Some(bytes) = custody.take_vec_allocation() else {
            return Err(custody);
        };
        let pointer = bytes.as_ptr();
        let length = bytes.len();
        // Bytes::from(Vec) preserves that actual Vec allocation. This new Bytes
        // has never been cloned; conversion back to mutable storage is unique.
        let bytes = bytes::Bytes::from(bytes)
            .try_into_mut()
            .expect("same newly moved unique Vec");
        debug_assert!(length == 0 || bytes.as_ptr() == pointer);
        let mut packet = Self::from_bytes(bytes);
        packet.inner._custody = Some(custody);
        Ok(packet)
    }
    /// The actual moved allocation keeps its original storage controller.
    /// Admitting a forward only changes that owner's memory lane/credits.
    pub fn prepare_owned_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), zero_traits::PacketDeriveError> {
        self.inner
            ._custody
            .as_mut()
            .ok_or(zero_traits::PacketDeriveError::Unreserved)?
            .prepare_transmit(frames, bytes)
    }
    pub fn try_derive_owned_buffers(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<zero_traits::PacketBuffer>, zero_traits::PacketDeriveError> {
        self.inner
            ._custody
            .as_ref()
            .ok_or(zero_traits::PacketDeriveError::Unreserved)?
            .try_derive_zeroed(lengths)
    }
    /// The owned producer has prepaid its one complete ciphertext allocation.
    /// Padding is written into that output payload, never another plaintext Vec.
    pub fn install_owned_padding(&mut self, padded: usize) -> Result<(), ()> {
        if !self.has_owned_custody()
            || padded < self.len()
            || padded - self.len() >= 16
            || padded % 16 != 0
        {
            return Err(());
        }
        self.inner._owned_padding = Some(padded);
        Ok(())
    }
    /// Retain the actual immutable/engine-backed IP source without copying it
    /// into a separate padded plaintext allocation. This placeholder is used
    /// only by the owned transport encapsulation path; ciphertext is prepaid.
    pub fn install_owned_plaintext(
        &mut self,
        source: zero_traits::PacketBuffer,
        padded: usize,
    ) -> Result<(), zero_traits::PacketBuffer> {
        if !self.inner.buf.is_empty()
            || self.inner._source.is_some()
            || source.is_empty()
            || padded < source.len()
            || padded - source.len() >= 16
            || padded % 16 != 0
        {
            return Err(source);
        }
        self.inner._source = Some(std::sync::Arc::new(source));
        self.inner._owned_padding = Some(padded);
        Ok(())
    }
    pub(crate) fn owned_plaintext(&self) -> &[u8] {
        if self.inner._owned_padding.is_some() {
            if let Some(source) = &self.inner._source {
                return source.as_ref().as_ref();
            }
        }
        &self.inner.buf
    }
    pub fn install_prepaid_cipher(&mut self, cipher: Packet) -> Result<(), Packet> {
        if self.inner._cipher.is_some() {
            return Err(cipher);
        }
        self.inner._cipher = Some(Box::new(cipher));
        Ok(())
    }
    /// Keep the original source allocation in the same queued plaintext; it is
    /// released only after that actual packet (and its ciphertext) ends.
    pub fn retain_source(
        &mut self,
        source: zero_traits::PacketBuffer,
    ) -> Result<(), zero_traits::PacketBuffer> {
        if self.inner._source.is_some() {
            return Err(source);
        }
        self.inner._source = Some(std::sync::Arc::new(source));
        Ok(())
    }
    /// A failed candidate authentication may release its own scratch while
    /// the original ciphertext stays with this same finite receive operation.
    pub fn retain_shared_source(
        &mut self,
        source: std::sync::Arc<zero_traits::PacketBuffer>,
    ) -> Result<(), std::sync::Arc<zero_traits::PacketBuffer>> {
        if self.inner._source.is_some() {
            return Err(source);
        }
        self.inner._source = Some(source);
        Ok(())
    }
    pub(crate) fn take_prepaid_cipher(&mut self, length: usize) -> Result<Option<Packet>, ()> {
        let Some(mut cipher) = self.inner._cipher.take() else {
            return Ok(None);
        };
        if cipher.len() < length {
            self.inner._cipher = Some(cipher);
            return Err(());
        }
        cipher.truncate(length);
        Ok(Some(*cipher))
    }
    pub(crate) fn retain_prior(&mut self, prior: Packet) {
        assert!(self.inner._prior.is_none(), "one actual prior plaintext");
        self.inner._prior = Some(Box::new(prior));
    }
}

impl<T: ?Sized> Packet<T> {
    pub(crate) fn owned_padding(&self) -> Option<usize> {
        self.inner._owned_padding
    }
    pub(crate) fn has_owned_custody(&self) -> bool {
        self.inner._custody.is_some()
            || self.inner._prior.is_some()
            || self.inner._cipher.is_some()
            || self.inner._source.is_some()
    }
}
