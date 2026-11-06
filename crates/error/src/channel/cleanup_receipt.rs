use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

/// One actual controller's stable cleanup-fault record. This is local delivery
/// evidence only, never native/view/task closure, Host ACK or resource credit.
/// The producer must keep all of that record's causes in the same retained
/// payload, including secondary causes learned while cleanup is still pending.
#[derive(Clone, Debug, Default)]
pub struct CleanupFailureReceipt(Arc<AtomicBool>);
impl CleanupFailureReceipt {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn is_acknowledged(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    pub fn same_receipt(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// The real bounded receiver must first persist this exact returned Error
    /// Arc and its stable payload. Equal text/fields or another receipt cannot
    /// acknowledge it. No diagnostic callback, task or IO operation runs here.
    pub fn acknowledge_retained(&self, retained: &Arc<ChannelError>) -> bool {
        if !retained
            .cleanup_failure_receipt()
            .is_some_and(|actual| self.same_receipt(actual))
        {
            return false;
        }
        self.0.store(true, Ordering::Release);
        true
    }
}

#[cfg(test)]
mod tests;
