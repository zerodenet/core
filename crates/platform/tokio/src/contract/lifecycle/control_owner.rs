//! The original erased controller Box followed by its concrete metadata debit.
//! No second Box, new executor/close protocol, native or task evidence exists.
use super::*;
use std::ops::{Deref, DerefMut};
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataCredit, PacketMetadataLayout};
pub struct BoxChannelControl {
    original: Box<dyn DynChannelControl>,
    _metadata: Option<PacketMetadataCredit>,
}
impl BoxChannelControl {
    pub fn from_prepaid<T: DynChannelControl + 'static>(
        original: T,
        credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, T, PacketMetadataCredit)> {
        let expected = PacketMetadataLayout::boxed::<T>();
        if credit.layout().allocations() < expected.allocations()
            || credit.layout().bytes() < expected.bytes()
        {
            return Err((PacketMemoryError::Capacity, original, credit));
        }
        Ok(Self {
            original: Box::new(original),
            _metadata: Some(credit),
        })
    }
}
impl<T: DynChannelControl + 'static> From<Box<T>> for BoxChannelControl {
    fn from(original: Box<T>) -> Self {
        Self {
            original,
            _metadata: None,
        }
    }
}
impl From<Box<dyn DynChannelControl>> for BoxChannelControl {
    fn from(original: Box<dyn DynChannelControl>) -> Self {
        Self {
            original,
            _metadata: None,
        }
    }
}
impl Deref for BoxChannelControl {
    type Target = dyn DynChannelControl;
    fn deref(&self) -> &Self::Target {
        &*self.original
    }
}
impl DerefMut for BoxChannelControl {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.original
    }
}
impl AsRef<dyn DynChannelControl> for BoxChannelControl {
    fn as_ref(&self) -> &(dyn DynChannelControl + 'static) {
        &*self.original
    }
}
impl AsMut<dyn DynChannelControl> for BoxChannelControl {
    fn as_mut(&mut self) -> &mut (dyn DynChannelControl + 'static) {
        &mut *self.original
    }
}
impl DynChannelControl for BoxChannelControl {
    fn revoked(&self) -> BoxFuture<'static, ()> {
        self.original.revoked()
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.original.try_close()
    }
    fn cancel(&self, reason: CloseReason) {
        self.original.cancel(reason);
    }
    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        self.original.wait_closed(deadline)
    }
}

#[cfg(test)]
mod tests;
