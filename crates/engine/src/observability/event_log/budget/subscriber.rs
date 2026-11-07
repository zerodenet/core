//! Same actual Engine queue, registration, original filter and byte/count books.
use super::*;
use std::{
    io,
    sync::{Arc, Mutex, Weak},
};
mod book;
mod limits;
mod registry;
mod storage;
mod totals;
pub(crate) use book::QueuedEvent;
pub(crate) use limits::SubscriberLimits;
pub(crate) use registry::{SubscriberRegistration, SubscriberRegistry};
#[derive(Debug)]
pub(crate) struct Subscriber {
    tx: SyncSender<QueuedEvent>,
    book: Arc<book::Book>,
    // Same original statistics counter; no payload-copy or replacement book.
    #[cfg(test)]
    pub(super) budget: Arc<AtomicUsize>,
    pub(super) stats_limit: usize,
    limits: SubscriberLimits,
}
impl Subscriber {
    pub(super) fn send(&self, event: &Arc<RawApiEvent>, bytes: usize) -> bool {
        if !matches_filter(event, &self.book.filter) {
            return true;
        }
        let mut limits = self.limits;
        limits.stats = self.stats_limit;
        let Ok(reservation) = self.book.reserve(bytes, limits) else {
            return true;
        };
        let queued = QueuedEvent {
            event: event.clone(),
            reservation,
        };
        match self.tx.try_send(queued) {
            Ok(()) | Err(TrySendError::Full(_)) => true,
            Err(TrySendError::Disconnected(_)) => false,
        }
    }
}
/// Cold preparation already retains the actual registration and storage quota.
/// Only this original constructs/installs its one std bounded queue.
pub(crate) struct PreparedSubscriber {
    original: Arc<book::Book>,
    registry: Weak<Mutex<SubscriberRegistry>>,
}
impl PreparedSubscriber {
    pub(crate) fn prepare(
        registry: &Arc<Mutex<SubscriberRegistry>>,
        filter: EventFilter,
    ) -> io::Result<Self> {
        Self::prepare_slots(registry, filter, 1024)
    }
    fn prepare_slots(
        registry: &Arc<Mutex<SubscriberRegistry>>,
        filter: EventFilter,
        slots: usize,
    ) -> io::Result<Self> {
        let mut entries = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let original = entries.prepare_book(filter, slots)?;
        Ok(Self {
            original,
            registry: Arc::downgrade(registry),
        })
    }
    pub(crate) fn install(
        &self,
        tx: SyncSender<QueuedEvent>,
    ) -> io::Result<SubscriberRegistration> {
        let registry = self.registry.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "original Engine publisher retired",
            )
        })?;
        let mut entries = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Cap changes revalidate all pending/retiring Books. Use this same
        // registry's current limits, not a stale preparation's admission caps.
        let limits = entries.limits;
        if !self.original.fits(limits) {
            return Err(totals::capacity());
        }
        entries.entries.push(Subscriber {
            tx,
            #[cfg(test)]
            budget: self.original.stats.clone(),
            book: self.original.clone(),
            stats_limit: limits.stats,
            limits,
        });
        drop(entries);
        Ok(SubscriberRegistration {
            original: self.original.clone(),
            registry: self.registry.clone(),
        })
    }
}

/// Specific same-Engine initial envelope/workspace credit; not a Control book.
/// The actual subscriber Book survives with its queued original.
pub(crate) struct SubscriberStorage {
    _original: Arc<book::Book>,
    registry: Weak<Mutex<SubscriberRegistry>>,
    credit: totals::Storage,
}
impl SubscriberRegistration {
    pub(crate) fn storage(&self) -> SubscriberStorage {
        SubscriberStorage {
            _original: self.original.clone(),
            registry: self.registry.clone(),
            credit: totals::Storage::new(self.original.root.clone()),
        }
    }
}
impl SubscriberStorage {
    pub(crate) fn grow(&mut self, bytes: usize) -> io::Result<()> {
        let registry = self.registry.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "original Engine publisher retired",
            )
        })?;
        let entries = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.credit.grow(bytes, entries.limits.storage_total)
    }
}

#[cfg(test)]
impl Subscriber {
    /// A genuine unit-level byte owner for its actual caller-constructed std
    /// channel. Production always prepares the original Engine book first.
    pub(super) fn new(tx: SyncSender<QueuedEvent>, filter: EventFilter, slots: usize) -> Self {
        let limits = SubscriberLimits::default();
        let root = Arc::new(totals::Totals::default());
        let book = book::Book::new(&root, filter, limits, slots).unwrap();
        Self {
            tx,
            budget: book.stats.clone(),
            book,
            stats_limit: limits.stats,
            limits,
        }
    }
}
#[cfg(test)]
impl EngineEventLog {
    pub(crate) fn test_subscriber(
        &self,
        tx: SyncSender<QueuedEvent>,
        filter: EventFilter,
        slots: usize,
    ) -> io::Result<(SubscriberRegistration, Arc<AtomicUsize>)> {
        let prepared = PreparedSubscriber::prepare_slots(&self.subscribers, filter, slots)?;
        let counter = prepared.original.stats.clone();
        let registration = prepared.install(tx)?;
        Ok((registration, counter))
    }
}

#[cfg(test)]
mod tests;
