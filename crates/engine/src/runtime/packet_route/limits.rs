//! Configure admission on the same original route table, without eviction.
use super::*;
use std::{io, sync::MutexGuard};
pub(in crate::runtime) struct PreparedLimit<'a> {
    owner: &'a PacketRoutes,
    _entries: MutexGuard<'a, BTreeMap<String, Weak<Control>>>,
    proposed: usize,
}
impl PacketRoutes {
    pub(in crate::runtime) fn new(limit: usize) -> Self {
        Self {
            next: AtomicU64::new(0),
            limit: AtomicUsize::new(limit),
            entries: Mutex::new(BTreeMap::new()),
        }
    }
    pub(in crate::runtime) fn prepare_limit(
        &self,
        proposed: usize,
    ) -> io::Result<PreparedLimit<'_>> {
        if !(1..=MAX_ROUTES).contains(&proposed) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid packet route limit",
            ));
        }
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let retained = entries
            .values()
            .filter_map(Weak::upgrade)
            .filter(|c| !(c.released.get() && c.responses.get()))
            .count();
        if retained > proposed {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("packet routes retain {retained} originals above proposed {proposed}",),
            ));
        }
        Ok(PreparedLimit {
            owner: self,
            _entries: entries,
            proposed,
        })
    }
}
impl PreparedLimit<'_> {
    pub(in crate::runtime) fn commit(&self) {
        self.owner.limit.store(self.proposed, Ordering::Relaxed);
    }
}
#[cfg(test)]
mod tests;
