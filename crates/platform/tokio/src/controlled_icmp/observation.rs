//! Same actual ICMP original; read-only observation survives provider reaping.
use super::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    OnceLock,
};
use zero_traits::packet_metadata::{
    PacketMemoryError, PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout,
};
#[derive(Default)]
struct State {
    original: OnceLock<Arc<Entry>>,
    started: AtomicBool,
    rejected: AtomicBool,
    changed: tokio::sync::Notify,
    depth: AtomicUsize,
    pending: AtomicBool,
}
#[derive(Clone)]
pub struct IcmpAttemptObservation(PacketMetadataArc<State>);
pub enum IcmpAttemptDisposition {
    Pending,
    KnownNoNative,
    Closed(IcmpCloseReceipt),
}
impl IcmpAttemptObservation {
    pub(crate) fn metadata_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataArc::<State>::layout()
    }
    pub(crate) fn from_prepaid(
        credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, PacketMetadataCredit)> {
        PacketMetadataArc::from_prepaid(State::default(), credit)
            .map(Self)
            .map_err(|(error, _, credit)| (error, credit))
    }
    pub(crate) fn start(&self) -> bool {
        self.0
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    pub(super) fn install(&self, entry: &Arc<Entry>) {
        assert!(
            self.0.original.set(entry.clone()).is_ok(),
            "one observed ICMP original"
        );
        self.notify();
    }
    /// Rejection before entry installation proves only this attempt acquired no
    /// original, never that an older unrelated Unknown original was closed.
    pub(crate) fn reject_unstarted(&self) {
        if self.0.original.get().is_none() {
            self.0.rejected.store(true, Ordering::Release);
        }
        self.notify();
    }
    fn notify(&self) {
        self.0.pending.store(true, Ordering::Release);
        self.flush();
    }
    fn flush(&self) {
        if self.0.depth.load(Ordering::SeqCst) == 0 && self.0.pending.swap(false, Ordering::AcqRel)
        {
            self.0.changed.notify_waiters()
        }
    }
    pub fn defer_notifications(&self) -> IcmpObservationNotifications {
        self.0.depth.fetch_add(1, Ordering::SeqCst);
        IcmpObservationNotifications(self.clone())
    }
    pub fn disposition(&self) -> IcmpAttemptDisposition {
        if let Some(entry) = self.0.original.get() {
            if entry.creation_running.load(Ordering::Acquire) {
                return IcmpAttemptDisposition::Pending;
            }
            let Some(native) = relay::NativeGuard::try_lock(entry) else {
                return IcmpAttemptDisposition::Pending;
            };
            return match &*native {
                Native::Closed => IcmpAttemptDisposition::Closed(IcmpCloseReceipt {
                    entry: entry.clone(),
                }),
                Native::NeverAcquired => IcmpAttemptDisposition::KnownNoNative,
                _ => IcmpAttemptDisposition::Pending,
            };
        }
        if self.0.rejected.load(Ordering::Acquire) {
            IcmpAttemptDisposition::KnownNoNative
        } else {
            IcmpAttemptDisposition::Pending
        }
    }
    pub fn view_returned(&self) -> bool {
        self.0.original.get().is_some_and(|entry| {
            !entry.creation_running.load(Ordering::Acquire)
                && !entry.view_live.load(Ordering::Acquire)
        })
    }
    pub async fn wait_original(&self) -> IcmpAttemptDisposition {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(entry) = self.0.original.get() {
                let native_changed = entry.changed.notified();
                tokio::pin!(native_changed);
                native_changed.as_mut().enable();
                match self.disposition() {
                    IcmpAttemptDisposition::Pending => {}
                    complete => return complete,
                }
                tokio::select! {_ = changed=>{}, _ = native_changed=>{}}
            } else {
                match self.disposition() {
                    IcmpAttemptDisposition::Pending => {}
                    complete => return complete,
                }
                changed.await;
            }
        }
    }
}

#[must_use]
pub struct IcmpObservationNotifications(IcmpAttemptObservation);
impl Drop for IcmpObservationNotifications {
    fn drop(&mut self) {
        if self.0 .0.depth.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.flush()
        }
    }
}

impl IcmpCloseReceipt {
    pub fn matches_attempt(&self, original: &IcmpAttemptObservation) -> bool {
        original
            .0
            .original
            .get()
            .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
    }
}
