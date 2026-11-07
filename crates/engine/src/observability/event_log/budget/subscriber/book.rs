//! One original filter and its queued/dequeued custody; no receiver or worker.
use super::{
    limits::SubscriberLimits,
    storage,
    totals::{self, Registration, Storage, Totals},
};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use zero_api::{EventFilter, RawApiEvent};
#[derive(Debug)]
pub(super) struct Book {
    pub filter: EventFilter,
    pub pending: AtomicUsize,
    pub stats: Arc<AtomicUsize>,
    pub root: Arc<Totals>,
    pub filter_heap: usize,
    // Both this actual Book and its weak registry observation retain the same
    // allocation charge. A tombstone cannot refund the still-allocated Arc.
    pub allocation: Arc<Storage>,
    // Payload/field allocations drop before their actual storage permission.
    _storage: Storage,
    _registration: Registration,
}
impl Book {
    pub fn new(
        root: &Arc<Totals>,
        filter: EventFilter,
        limits: SubscriberLimits,
        physical_slots: usize,
    ) -> io::Result<Arc<Self>> {
        let heap = storage::filter_bytes(&filter)?;
        if heap > limits.filter {
            return Err(totals::capacity());
        }
        let registration = Registration::new(root, limits.registrations)?;
        let mut credit = Storage::new(root.clone());
        // Filter Vec/String capacity plus the concrete fixed Book/Arc and its
        // original statistics counter; allocator overhead is not RSS accounting.
        let fixed = std::mem::size_of::<Self>()
            .checked_add(4 * std::mem::size_of::<usize>())
            .and_then(|n| n.checked_add(std::mem::size_of::<AtomicUsize>()))
            .ok_or_else(storage::overflow)?;
        let mut allocation = Storage::new(root.clone());
        allocation.grow(
            fixed
                .checked_add(std::mem::size_of::<Storage>() + 2 * std::mem::size_of::<usize>())
                .ok_or_else(storage::overflow)?,
            limits.storage_total,
        )?;
        let bytes = heap
            .checked_add(storage::queue_array_bytes(physical_slots)?)
            .ok_or_else(storage::overflow)?;
        credit.grow(bytes, limits.storage_total)?;
        Ok(Arc::new(Self {
            filter,
            pending: AtomicUsize::new(0),
            stats: Arc::new(AtomicUsize::new(0)),
            root: root.clone(),
            filter_heap: heap,
            allocation: Arc::new(allocation),
            _storage: credit,
            _registration: registration,
        }))
    }
    pub fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        limits: SubscriberLimits,
    ) -> io::Result<Reservation> {
        totals::add(&self.pending, 1, limits.pending)?;
        if let Err(error) = totals::add(&self.root.events, 1, limits.pending_total) {
            totals::release(&self.pending, 1);
            return Err(error);
        }
        if let Err(error) = totals::add(&self.stats, bytes, limits.stats) {
            totals::release(&self.root.events, 1);
            totals::release(&self.pending, 1);
            return Err(error);
        }
        if let Err(error) = totals::add(&self.root.stats, bytes, limits.stats_total) {
            totals::release(&self.stats, bytes);
            totals::release(&self.root.events, 1);
            totals::release(&self.pending, 1);
            return Err(error);
        }
        Ok(Reservation {
            original: self.clone(),
            bytes,
        })
    }
    pub fn fits(&self, limits: SubscriberLimits) -> bool {
        self.pending.load(Ordering::Acquire) <= limits.pending
            && self.stats.load(Ordering::Acquire) <= limits.stats
            && self.filter_heap <= limits.filter
    }
}
pub(crate) struct Reservation {
    original: Arc<Book>,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        totals::release(&self.original.stats, self.bytes);
        totals::release(&self.original.root.stats, self.bytes);
        totals::release(&self.original.pending, 1);
        totals::release(&self.original.root.events, 1);
    }
}
/// Field order returns the actual shared event before its old reservation;
/// into_shared explicitly transfers that original to a prepaid new receiver.
pub(crate) struct QueuedEvent {
    pub(crate) event: Arc<RawApiEvent>,
    pub(super) reservation: Reservation,
}
impl QueuedEvent {
    pub(crate) fn into_event(self) -> RawApiEvent {
        Arc::unwrap_or_clone(self.event)
    }
}
