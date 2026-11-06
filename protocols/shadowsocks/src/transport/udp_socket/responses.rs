//! The original receive future delivers directly to its existing cache owner.
use super::*;
use std::sync::{Mutex, Weak};
/// A response destination held by the actual cache entry, not a worker task.
pub type ShadowsocksUdpResponseSink = dyn Fn(ShadowsocksUdpResponse) + Send + Sync;
#[derive(Default)]
pub(super) struct Responses {
    sink: Mutex<Option<Weak<ShadowsocksUdpResponseSink>>>,
}
impl Responses {
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
