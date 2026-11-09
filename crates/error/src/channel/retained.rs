//! Pure diagnostic custody; no native/Scope/receiver authority.
use super::*;
use std::ops::Deref;
use zero_traits::packet_metadata::{
    PacketMemoryError, PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout,
    PacketMetadataWeak,
};
#[derive(Clone)]
pub struct RetainedError(Storage);
#[derive(Clone)]
enum Storage {
    Library(Arc<ChannelError>),
    Prepaid(PacketMetadataArc<ChannelError>),
}
#[derive(Clone)]
pub struct WeakRetainedError(WeakStorage);
#[derive(Clone)]
enum WeakStorage {
    Library(std::sync::Weak<ChannelError>),
    Prepaid(PacketMetadataWeak<ChannelError>),
}
impl RetainedError {
    pub fn metadata_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataArc::<ChannelError>::layout()
    }
    pub fn from_prepaid(
        error: ChannelError,
        credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, ChannelError, PacketMetadataCredit)> {
        PacketMetadataArc::from_prepaid(error, credit).map(|value| Self(Storage::Prepaid(value)))
    }
    /// Original independent library responsibility; never a missing Host budget fallback.
    pub fn from_library(error: Arc<ChannelError>) -> Self {
        Self(Storage::Library(error))
    }
    pub fn library_arc(&self) -> Option<&Arc<ChannelError>> {
        match &self.0 {
            Storage::Library(value) => Some(value),
            Storage::Prepaid(_) => None,
        }
    }
    pub(super) fn as_error(&self) -> &(dyn Error + 'static) {
        match &self.0 {
            Storage::Library(value) => value,
            Storage::Prepaid(value) => value.as_ref(),
        }
    }
    pub fn is_prepaid(&self) -> bool {
        matches!(&self.0, Storage::Prepaid(_))
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        std::ptr::eq::<ChannelError>(&**self, &**other)
    }
    pub fn downgrade(&self) -> WeakRetainedError {
        WeakRetainedError(match &self.0 {
            Storage::Library(value) => WeakStorage::Library(Arc::downgrade(value)),
            Storage::Prepaid(value) => WeakStorage::Prepaid(PacketMetadataArc::downgrade(value)),
        })
    }
}
impl From<Arc<ChannelError>> for RetainedError {
    fn from(error: Arc<ChannelError>) -> Self {
        Self::from_library(error)
    }
}
impl Deref for RetainedError {
    type Target = ChannelError;
    fn deref(&self) -> &ChannelError {
        match &self.0 {
            Storage::Library(value) => value,
            Storage::Prepaid(value) => value,
        }
    }
}
impl AsRef<ChannelError> for RetainedError {
    fn as_ref(&self) -> &ChannelError {
        self
    }
}
impl WeakRetainedError {
    pub fn upgrade(&self) -> Option<RetainedError> {
        match &self.0 {
            WeakStorage::Library(value) => value
                .upgrade()
                .map(|value| RetainedError(Storage::Library(value))),
            WeakStorage::Prepaid(value) => value
                .upgrade()
                .map(|value| RetainedError(Storage::Prepaid(value))),
        }
    }
}
impl fmt::Debug for RetainedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedError")
            .field("prepaid", &self.is_prepaid())
            .field("stage", &self.stage)
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for RetainedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}
impl Error for RetainedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&**self)
    }
}

#[cfg(test)]
mod tests;
