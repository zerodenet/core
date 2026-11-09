//! Concrete original-owner completion notifications, deferred across Host locks.
use super::*;
use std::{
    ops::Deref,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataArc};
#[derive(Default)]
pub(super) struct Signal {
    changed: tokio::sync::Notify,
    depth: AtomicUsize,
    pending: AtomicBool,
}
#[derive(Clone)]
pub(super) enum Identity {
    Library(Arc<Signal>),
    Hosted(PacketMetadataArc<Signal>),
}
impl Identity {
    fn signal(&self) -> &Signal {
        match self {
            Self::Library(x) => x,
            Self::Hosted(x) => x,
        }
    }
    pub(super) fn library() -> Self {
        Self::Library(Arc::default())
    }
    pub(super) fn hosted(
        memory: &crate::contract::component::PacketDriverMetadataView,
    ) -> Result<Self, PacketMemoryError> {
        let credit = memory.reserve(PacketMetadataArc::<Signal>::layout()?)?;
        PacketMetadataArc::from_prepaid(Signal::default(), credit)
            .map(Self::Hosted)
            .map_err(|(error, _, _)| error)
    }
    pub(super) fn same(&self, other: &Self) -> bool {
        std::ptr::eq(self.signal(), other.signal())
    }
    pub(super) fn notify_waiters(&self) {
        self.signal().pending.store(true, Ordering::Release);
        self.flush()
    }
    fn flush(&self) {
        if self.signal().depth.load(Ordering::SeqCst) == 0
            && self.signal().pending.swap(false, Ordering::AcqRel)
        {
            self.signal().changed.notify_waiters()
        }
    }
    pub(super) fn defer(&self) -> IcmpNotifications {
        self.signal().depth.fetch_add(1, Ordering::SeqCst);
        IcmpNotifications(self.clone())
    }
}
impl Deref for Identity {
    type Target = tokio::sync::Notify;
    fn deref(&self) -> &Self::Target {
        &self.signal().changed
    }
}
/// Defers only these concrete owner's completion callbacks, no IO/close proof.
/// The enclosing Host mutex must be released before this guard is dropped.
pub struct IcmpNotifications(Identity);
impl Drop for IcmpNotifications {
    fn drop(&mut self) {
        if self.0.signal().depth.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.flush()
        }
    }
}
