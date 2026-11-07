//! This is the existing Engine subscriber registry, including its actual storage.
use super::{
    book::Book,
    limits::SubscriberLimits,
    storage,
    totals::{self, Storage, Totals},
    Subscriber,
};
use std::{
    io,
    ops::{Deref, DerefMut},
    sync::{atomic::Ordering, Arc, Mutex, Weak},
};
#[derive(Debug)]
struct OriginalBook {
    // Weak Arc allocation is destroyed before its same allocation credit.
    original: Weak<Book>,
    _allocation: Arc<Storage>,
}
#[derive(Debug)]
pub(crate) struct SubscriberRegistry {
    pub(super) entries: Vec<Subscriber>,
    books: Vec<OriginalBook>,
    pub(super) limits: SubscriberLimits,
    pub(super) root: Arc<Totals>,
    // Fields above release their actual vector buffers before this permission.
    workspace: Storage,
}
impl Default for SubscriberRegistry {
    fn default() -> Self {
        let root = Arc::new(Totals::default());
        Self {
            entries: Vec::new(),
            books: Vec::new(),
            limits: Default::default(),
            workspace: Storage::new(root.clone()),
            root,
        }
    }
}
impl Deref for SubscriberRegistry {
    type Target = [Subscriber];
    fn deref(&self) -> &[Subscriber] {
        &self.entries
    }
}
impl DerefMut for SubscriberRegistry {
    fn deref_mut(&mut self) -> &mut [Subscriber] {
        &mut self.entries
    }
}
impl SubscriberRegistry {
    fn storage_bytes(&self) -> io::Result<usize> {
        storage::vector_bytes::<Subscriber>(self.entries.capacity())?
            .checked_add(storage::vector_bytes::<OriginalBook>(
                self.books.capacity(),
            )?)
            .ok_or_else(storage::overflow)
    }
    pub(super) fn compact(&mut self) {
        self.books
            .retain(|record| record.original.strong_count() != 0);
        // Pending preparations already paid this same registry slot before
        // their channel exists. A receiver Drop cannot release that vacancy.
        self.entries
            .shrink_to(self.root.registrations.load(Ordering::Acquire));
        self.books.shrink_to_fit();
        let held = self
            .storage_bytes()
            .expect("checked actual registry storage");
        if held < self.workspace.held() {
            self.workspace.shrink(self.workspace.held() - held);
        }
    }
    pub(super) fn prepare_book(
        &mut self,
        filter: zero_api::EventFilter,
        physical_slots: usize,
    ) -> io::Result<Arc<Book>> {
        self.compact();
        self.limits.validate()?;
        let book = Book::new(&self.root, filter, self.limits, physical_slots)?;
        // All pending/retiring Books still hold their actual registration. This
        // upper bound reserves the active vector before constructing a channel.
        let live_needed = self.root.registrations.load(Ordering::Acquire);
        let books_needed = self
            .books
            .len()
            .checked_add(1)
            .ok_or_else(storage::overflow)?;
        let live_cap = self.entries.capacity().max(live_needed);
        let book_cap = self.books.capacity().max(books_needed);
        let expected = storage::vector_bytes::<Subscriber>(live_cap)?
            .checked_add(storage::vector_bytes::<OriginalBook>(book_cap)?)
            .ok_or_else(storage::overflow)?;
        let old = self.workspace.held();
        self.workspace.grow(
            expected.checked_sub(old).expect("same vector workspace"),
            self.limits.storage_total,
        )?;
        let allocated = self
            .entries
            .try_reserve_exact(live_cap.saturating_sub(self.entries.len()))
            .and_then(|_| {
                self.books
                    .try_reserve_exact(book_cap.saturating_sub(self.books.len()))
            });
        // Allocation failure may leave the first actual Vec larger. Never
        // refund a requested delta without first observing its real capacity.
        let actual = self.storage_bytes()?;
        if actual < self.workspace.held() {
            self.workspace.shrink(self.workspace.held() - actual);
        }
        if actual > self.workspace.held() {
            self.workspace
                .record_owned_growth(actual - self.workspace.held());
        }
        allocated.map_err(io::Error::other)?;
        if self.root.storage.load(Ordering::Acquire) > self.limits.storage_total {
            return Err(totals::capacity());
        }
        self.books.push(OriginalBook {
            original: Arc::downgrade(&book),
            _allocation: book.allocation.clone(),
        });
        Ok(book)
    }
    pub(crate) fn current_limits(&self) -> SubscriberLimits {
        self.limits
    }
    pub(crate) fn check_limits(&mut self, limits: SubscriberLimits) -> io::Result<()> {
        self.compact();
        limits.validate()?;
        if self.root.registrations.load(Ordering::Acquire) > limits.registrations
            || self.root.events.load(Ordering::Acquire) > limits.pending_total
            || self.root.stats.load(Ordering::Acquire) > limits.stats_total
            || self.root.storage.load(Ordering::Acquire) > limits.storage_total
            || self
                .books
                .iter()
                .filter_map(|record| record.original.upgrade())
                .any(|book| !book.fits(limits))
        {
            return Err(totals::capacity());
        }
        Ok(())
    }
    // Only after check_limits with this very registry guard still held.
    pub(crate) fn install_limits(&mut self, limits: SubscriberLimits) {
        self.limits = limits;
        for entry in &mut self.entries {
            entry.limits = limits;
            entry.stats_limit = limits.stats;
        }
    }
    pub(crate) fn send(&mut self, event: &Arc<zero_api::RawApiEvent>, bytes: usize) {
        // These concrete fields contain only std channel metadata, the immutable
        // JSON event/filter and atomic counters; no native close or user waker.
        self.entries.retain(|entry| entry.send(event, bytes));
    }
    pub(super) fn remove(&mut self, original: &Arc<Book>) -> Option<Subscriber> {
        let index = self
            .entries
            .iter()
            .position(|entry| Arc::ptr_eq(&entry.book, original))?;
        let removed = self.entries.swap_remove(index);
        self.compact();
        Some(removed)
    }
}
/// Logical registration only. The actual queue originals independently keep
/// their Book/count/bytes/storage through dequeue and receiver retirement.
pub(crate) struct SubscriberRegistration {
    pub(super) original: Arc<Book>,
    pub(super) registry: Weak<Mutex<SubscriberRegistry>>,
}
impl SubscriberRegistration {
    pub(crate) fn filter(&self) -> &zero_api::EventFilter {
        &self.original.filter
    }
}
impl Drop for SubscriberRegistration {
    fn drop(&mut self) {
        let removed = self.registry.upgrade().and_then(|registry| {
            let removed = registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.original);
            removed
        });
        // Original Sender and Book destruction may release channel waiters.
        drop(removed);
    }
}

#[cfg(test)]
impl SubscriberRegistration {
    pub(crate) fn original_pending_for_test(&self) -> usize {
        self.original.pending.load(Ordering::Acquire)
    }
    pub(crate) fn original_storage_for_test(&self) -> usize {
        self.original.root.storage.load(Ordering::Acquire)
    }
}
