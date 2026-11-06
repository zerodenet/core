use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

/// One actual controller's stable cleanup-fault record. This is local delivery
/// evidence only, never native/view/task closure, Host ACK or resource credit.
/// The producer must keep all of that record's causes in the same retained
/// payload, including secondary causes learned while cleanup is still pending.
#[derive(Clone, Debug, Default)]
pub struct CleanupFailureReceipt(Arc<State>);
#[derive(Debug, Default)]
struct State {
    acknowledged: AtomicBool,
    payload: OnceLock<Arc<dyn Error + Send + Sync>>,
}
impl CleanupFailureReceipt {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn is_acknowledged(&self) -> bool {
        self.0.acknowledged.load(Ordering::Acquire)
    }
    pub fn same_receipt(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// Bind the controller's stable finite cause record once, before delivery.
    /// Every copied receipt then retains that exact record itself, even if a
    /// diagnostic wrapper changes. It must contain no IO/controller/Scope.
    pub fn bind_payload(&self, payload: Arc<dyn Error + Send + Sync>) -> bool {
        if let Some(actual) = self.0.payload.get() {
            return Arc::ptr_eq(actual, &payload);
        }
        match self.0.payload.set(payload) {
            Ok(()) => true,
            Err(payload) => self
                .0
                .payload
                .get()
                .is_some_and(|actual| Arc::ptr_eq(actual, &payload)),
        }
    }
    /// The real bounded receiver must first persist this exact returned Error
    /// Arc and its stable payload. Equal text/fields or another receipt cannot
    /// acknowledge it. No diagnostic callback, task or IO operation runs here.
    pub fn acknowledge_retained(&self, retained: &Arc<ChannelError>) -> bool {
        if self.0.payload.get().is_none()
            || !retained
                .cleanup_failure_receipt()
                .is_some_and(|actual| self.same_receipt(actual))
        {
            return false;
        }
        self.0.acknowledged.store(true, Ordering::Release);
        true
    }
}

#[cfg(test)]
mod tests;
