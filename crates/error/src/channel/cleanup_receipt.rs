use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

/// One actual controller's stable cleanup-fault record. This is local delivery
/// evidence only, never native/view/task closure, Host ACK or resource credit.
/// The producer must keep all of that record's causes in the same retained
/// payload, including secondary causes learned while cleanup is still pending.
#[derive(Clone, Debug, Default)]
pub struct CleanupFailureReceipt(Storage);
#[derive(Clone)]
enum Storage {
    Library(Arc<State>),
    Prepaid(zero_traits::packet_metadata::PacketMetadataArc<State>),
}
impl Default for Storage {
    fn default() -> Self {
        Self::Library(Arc::default())
    }
}
impl std::ops::Deref for Storage {
    type Target = State;
    fn deref(&self) -> &State {
        match self {
            Self::Library(value) => value,
            Self::Prepaid(value) => value,
        }
    }
}
impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CleanupFaultOriginal")
            .field("acknowledged", &self.acknowledged.load(Ordering::Acquire))
            .field("bound", &self.payload.get().is_some())
            .finish()
    }
}
#[derive(Debug, Default)]
struct State {
    acknowledged: AtomicBool,
    payload: OnceLock<Payload>,
}
#[derive(Debug)]
enum Payload {
    Library(Arc<dyn Error + Send + Sync>),
    Prepaid(zero_traits::packet_metadata::PacketMetadataError),
}
impl CleanupFailureReceipt {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn is_acknowledged(&self) -> bool {
        self.0.acknowledged.load(Ordering::Acquire)
    }
    pub fn same_receipt(&self, other: &Self) -> bool {
        std::ptr::eq::<State>(&*self.0, &*other.0)
    }
    /// Bind the controller's stable finite cause record once, before delivery.
    /// Every copied receipt then retains that exact record itself, even if a
    /// diagnostic wrapper changes. It must contain no IO/controller/Scope.
    pub fn bind_payload(&self, payload: Arc<dyn Error + Send + Sync>) -> bool {
        self.bind(Payload::Library(payload))
    }
    pub fn bind_metadata_payload(
        &self,
        payload: zero_traits::packet_metadata::PacketMetadataError,
    ) -> bool {
        self.bind(Payload::Prepaid(payload))
    }
    fn bind(&self, payload: Payload) -> bool {
        fn same(a: &Payload, b: &Payload) -> bool {
            match (a, b) {
                (Payload::Library(a), Payload::Library(b)) => Arc::ptr_eq(a, b),
                (Payload::Prepaid(a), Payload::Prepaid(b)) => a.ptr_eq(b),
                _ => false,
            }
        }
        if let Some(actual) = self.0.payload.get() {
            return same(actual, &payload);
        }
        match self.0.payload.set(payload) {
            Ok(()) => true,
            Err(payload) => same(self.0.payload.get().unwrap(), &payload),
        }
    }
    pub fn metadata_layout() -> Result<
        zero_traits::packet_metadata::PacketMetadataLayout,
        zero_traits::packet_metadata::PacketMemoryError,
    > {
        zero_traits::packet_metadata::PacketMetadataArc::<State>::layout()
    }
    pub fn from_prepaid(
        credit: zero_traits::packet_metadata::PacketMetadataCredit,
    ) -> Result<
        Self,
        (
            zero_traits::packet_metadata::PacketMemoryError,
            zero_traits::packet_metadata::PacketMetadataCredit,
        ),
    > {
        zero_traits::packet_metadata::PacketMetadataArc::from_prepaid(State::default(), credit)
            .map(|value| Self(Storage::Prepaid(value)))
            .map_err(|(source, _, credit)| (source, credit))
    }
    /// The real bounded receiver must first persist this exact returned Error
    /// Arc and its stable payload. Equal text/fields or another receipt cannot
    /// acknowledge it. No diagnostic callback, task or IO operation runs here.
    pub fn acknowledge_retained(&self, retained: &impl AsRef<ChannelError>) -> bool {
        let retained = retained.as_ref();
        if self.0.payload.get().is_none()
            || !retained
                .cleanup_failure_receipt()
                .is_some_and(|actual| self.same_receipt(actual))
        {
            return false;
        }
        self.0.acknowledged.store(true, Ordering::Release);
        true
    }
}

#[cfg(test)]
mod tests;
