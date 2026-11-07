//! Counters on the same Engine book, released only by their actual originals.
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
#[derive(Debug, Default)]
pub(super) struct Totals {
    pub registrations: AtomicUsize,
    pub events: AtomicUsize,
    pub stats: AtomicUsize,
    pub storage: AtomicUsize,
}
pub(super) fn capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "original Engine subscriber capacity exhausted",
    )
}
pub(super) fn add(counter: &AtomicUsize, amount: usize, maximum: usize) -> io::Result<()> {
    counter
        .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(amount).filter(|next| *next <= maximum)
        })
        .map(|_| ())
        .map_err(|_| capacity())
}
pub(super) fn release(counter: &AtomicUsize, amount: usize) {
    let previous = counter.fetch_sub(amount, Ordering::AcqRel);
    assert!(
        previous >= amount,
        "same actual subscriber reservation returned once"
    );
}
#[derive(Debug)]
pub(super) struct Storage {
    root: Arc<Totals>,
    held: usize,
}
impl Storage {
    pub fn new(root: Arc<Totals>) -> Self {
        Self { root, held: 0 }
    }
    pub fn grow(&mut self, amount: usize, maximum: usize) -> io::Result<()> {
        let next = self
            .held
            .checked_add(amount)
            .ok_or_else(super::storage::overflow)?;
        add(&self.root.storage, amount, maximum)?;
        self.held = next;
        Ok(())
    }
    // Call only after the actual retained Vec/filter storage was freed.
    pub fn shrink(&mut self, amount: usize) {
        self.held = self.held.checked_sub(amount).expect("same actual storage");
        release(&self.root.storage, amount);
    }
    // Vec::try_reserve_exact normally returns exactly its prepaid capacity.
    // If the actual allocator exposes more, keep that real storage charged
    // even above admission, reject this candidate, and allow compact/grow.
    pub fn record_owned_growth(&mut self, amount: usize) {
        self.held = self
            .held
            .checked_add(amount)
            .expect("checked actual subscriber metadata");
        self.root
            .storage
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount)
            })
            .expect("bounded actual subscriber allocation");
    }
    pub fn held(&self) -> usize {
        self.held
    }
}
impl Drop for Storage {
    fn drop(&mut self) {
        release(&self.root.storage, self.held);
    }
}
#[derive(Debug)]
pub(super) struct Registration {
    root: Arc<Totals>,
}
impl Registration {
    pub fn new(root: &Arc<Totals>, maximum: usize) -> io::Result<Self> {
        add(&root.registrations, 1, maximum)?;
        Ok(Self { root: root.clone() })
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        release(&self.root.registrations, 1);
    }
}
