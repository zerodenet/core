//! The original receive future delivers directly to its existing cache owner.
use super::*;
use std::sync::{Mutex, Weak};
/// A response destination held by the actual cache entry, not a worker task.
pub type ShadowsocksUdpResponseSink = dyn Fn(ShadowsocksUdpResponse) + Send + Sync;
pub(crate) mod admission;
pub(crate) mod budget;
pub use budget::ShadowsocksUdpPendingLimits;
mod pending;
pub use pending::ShadowsocksUdpPendingResponse;
pub(super) struct Responses {
    pending: Mutex<pending::Pending>,
    closed: std::sync::atomic::AtomicBool,
    budget: Arc<budget::PendingBudget>,
    capacity: Arc<tokio::sync::Semaphore>,
    sink: Mutex<Option<Weak<ShadowsocksUdpResponseSink>>>,
}
impl Default for Responses {
    fn default() -> Self {
        Self::with_budget(4096, Arc::default())
    }
}
impl Responses {
    pub(super) fn with_budget(limit: usize, budget: Arc<budget::PendingBudget>) -> Self {
        Self {
            pending: Default::default(),
            closed: Default::default(),
            budget,
            capacity: pending::capacity(limit),
            sink: Default::default(),
        }
    }

    pub(super) fn install(&self, sink: &Arc<ShadowsocksUdpResponseSink>) -> bool {
        let mut current = self.sink.lock().unwrap();
        if let Some(actual) = current.as_ref().and_then(Weak::upgrade) {
            // Two live cache entries must not steal each other's FIFO waiters.
            return Arc::ptr_eq(&actual, sink);
        }
        *current = Some(Arc::downgrade(sink));
        true
    }
    pub(super) fn deliver(
        &self,
        response: ShadowsocksUdpResponse,
        subscribers: &broadcast::Sender<ShadowsocksUdpResponse>,
    ) -> bool {
        let response = match self.deliver_pending(response) {
            Ok(()) => return true,
            Err(response) => response,
        };
        let sink = self.sink.lock().unwrap().as_ref().and_then(Weak::upgrade);
        if let Some(sink) = sink {
            // Never invoke the actual cache callback while holding protocol
            // metadata locks. Existing explicit subscriptions remain usable.
            if subscribers.receiver_count() != 0 {
                let _ = subscribers.send(response.clone());
            }
            sink(response);
            true
        } else {
            subscribers.send(response).is_ok()
        }
    }
}
