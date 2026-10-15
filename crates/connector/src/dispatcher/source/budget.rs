//! Connector-only admission gauges. No protocol or traffic counters live here.
use super::super::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zero_api::SourceBufferStatus;

pub(in crate::dispatcher) struct SourceBudget {
    pub permits: Arc<Semaphore>,
    pub capacity: usize,
    pub bytes: u32,
    events: AtomicU64,
    used_bytes: AtomicU64,
    high_events: AtomicU64,
    high_bytes: AtomicU64,
    pub waiting_bytes: AtomicU64,
    oversized: AtomicU64,
    pub paused: AtomicBool,
}

impl SourceBudget {
    pub(super) fn new(capacity: usize, bytes: u32) -> Arc<Self> {
        Arc::new(Self {
            permits: Arc::new(Semaphore::new(bytes as usize)),
            capacity,
            bytes,
            events: AtomicU64::new(0),
            used_bytes: AtomicU64::new(0),
            high_events: AtomicU64::new(0),
            high_bytes: AtomicU64::new(0),
            waiting_bytes: AtomicU64::new(0),
            oversized: AtomicU64::new(0),
            paused: AtomicBool::new(false),
        })
    }
    pub(super) fn reserve(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
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
        self.permits.close();
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
    _permit: OwnedSemaphorePermit,
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
