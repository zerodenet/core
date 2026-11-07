//! Connector-only admission gauges. No protocol or traffic counters live here.
use super::super::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zero_api::SourceBufferStatus;

pub(in crate::dispatcher) struct SourceBudget {
    permits: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    pub capacity: usize,
    pub bytes: u32,
    events: AtomicU64,
    used_bytes: AtomicU64,
    high_events: AtomicU64,
    high_bytes: AtomicU64,
    pub waiting_bytes: AtomicU64,
    oversized: AtomicU64,
    pub paused: AtomicBool,
    changed: tokio::sync::Notify,
}

impl SourceBudget {
    pub(in crate::dispatcher) fn new(capacity: usize, bytes: u32) -> Arc<Self> {
        let capacity = capacity.clamp(1, Semaphore::MAX_PERMITS);
        let bytes = bytes.clamp(1, Semaphore::MAX_PERMITS.min(u32::MAX as usize) as u32);
        Arc::new(Self {
            permits: Arc::new(Semaphore::new(bytes as usize)),
            slots: Arc::new(Semaphore::new(capacity)),
            capacity,
            bytes,
            events: AtomicU64::new(0),
            used_bytes: AtomicU64::new(0),
            high_events: AtomicU64::new(0),
            high_bytes: AtomicU64::new(0),
            waiting_bytes: AtomicU64::new(0),
            oversized: AtomicU64::new(0),
            paused: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        })
    }
    pub(super) async fn acquire(
        self: &Arc<Self>,
        bytes: u32,
    ) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), tokio::sync::AcquireError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.permits.is_closed() {
                return self.acquire_pair(bytes).await;
            }
            if self.paused.load(Ordering::Acquire) {
                changed.await;
                continue;
            }
            tokio::select! {
                biased;
                _ = &mut changed => continue,
                result = self.acquire_pair(bytes) => {
                    if self.paused.load(Ordering::Acquire) {
                        drop(result);
                        continue;
                    }
                    return result;
                }
            }
        }
    }
    async fn acquire_pair(
        &self,
        bytes: u32,
    ) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), tokio::sync::AcquireError> {
        let slot = self.slots.clone().acquire_owned().await?;
        let bytes = self.permits.clone().acquire_many_owned(bytes).await?;
        Ok((slot, bytes))
    }
    #[cfg(feature = "kernel-events")]
    pub(super) fn pause_replay(&self) {
        self.paused.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    #[cfg(feature = "kernel-events")]
    pub(super) fn resume_live(&self) {
        self.paused.store(false, Ordering::Release);
        self.changed.notify_waiters();
    }
    #[cfg(feature = "kernel-events")]
    pub(super) fn try_reserve_event(
        self: &Arc<Self>,
        event: &RawApiEvent,
    ) -> Option<SourceReservation> {
        let size = super::super::memory::serialized_bytes(event);
        let bytes = size.min(u64::from(self.bytes)).max(1) as u32;
        let slot = self.slots.clone().try_acquire_owned().ok()?;
        let permit = self.permits.clone().try_acquire_many_owned(bytes).ok()?;
        Some(self.reserve((slot, permit), size))
    }
    pub(super) fn reserve(
        self: &Arc<Self>,
        permit: (OwnedSemaphorePermit, OwnedSemaphorePermit),
        size: u64,
    ) -> SourceReservation {
        let bytes = size.min(u64::from(self.bytes)).max(1);
        let events = self.events.fetch_add(1, Ordering::Relaxed) + 1;
        let used = self.used_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.high_events.fetch_max(events, Ordering::Relaxed);
        self.high_bytes.fetch_max(used, Ordering::Relaxed);
        if size > u64::from(self.bytes) {
            self.oversized.fetch_add(1, Ordering::Relaxed);
        }
        SourceReservation {
            _permit: permit,
            budget: self.clone(),
            bytes,
        }
    }
    pub(in crate::dispatcher) fn close(&self) {
        self.slots.close();
        self.permits.close();
        self.changed.notify_waiters();
    }
    pub(in crate::dispatcher) fn snapshot(&self) -> SourceBufferStatus {
        SourceBufferStatus {
            capacity_events: self.capacity as u64,
            byte_budget: self.bytes as u64,
            reserved_events: self.events.load(Ordering::Relaxed),
            reserved_bytes: self.used_bytes.load(Ordering::Relaxed),
            high_water_events: self.high_events.load(Ordering::Relaxed),
            high_water_bytes: self.high_bytes.load(Ordering::Relaxed),
            waiting_event_bytes: self.waiting_bytes.load(Ordering::Relaxed),
            oversized_events: self.oversized.load(Ordering::Relaxed),
            paused: self.paused.load(Ordering::Relaxed)
                || self.waiting_bytes.load(Ordering::Relaxed) > 0,
        }
    }
}

pub(in crate::dispatcher) struct SourceReservation {
    _permit: (OwnedSemaphorePermit, OwnedSemaphorePermit),
    budget: Arc<SourceBudget>,
    bytes: u64,
}
impl Drop for SourceReservation {
    fn drop(&mut self) {
        self.budget.events.fetch_sub(1, Ordering::Relaxed);
        self.budget
            .used_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}
