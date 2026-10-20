use super::*;
use std::fmt;
use zero_traits::{IoObserver, PacketDropReason};
pub(in crate::quic::owned) struct Observer {
    original: Option<Arc<dyn IoObserver>>,
    broker: Arc<Broker>,
}
impl Observer {
    pub(in crate::quic::owned) fn new(original: Arc<dyn IoObserver>, broker: Arc<Broker>) -> Self {
        Self {
            original: Some(original),
            broker,
        }
    }
    fn invoke(&self, callback: impl FnOnce(&dyn IoObserver)) {
        let original = self.original.as_ref().unwrap();
        self.broker.invoke(|| callback(original.as_ref()));
    }
}
impl fmt::Debug for Observer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeferredQuinnObserver")
            .finish_non_exhaustive()
    }
}
impl IoObserver for Observer {
    fn stream_boundary(&self) {
        self.invoke(|o| o.stream_boundary());
    }
    fn datagram_boundary(&self) {
        self.invoke(|o| o.datagram_boundary());
    }
    fn receive_coverage_lost(&self) {
        self.invoke(|o| o.receive_coverage_lost());
    }
    fn received_datagram(&self, bytes: usize) {
        self.invoke(|o| o.received_datagram(bytes));
    }
    fn sent_datagram(&self, bytes: usize) {
        self.invoke(|o| o.sent_datagram(bytes));
    }
    fn received(&self, bytes: usize) {
        self.invoke(|o| o.received(bytes));
    }
    fn sent(&self, bytes: usize) {
        self.invoke(|o| o.sent(bytes));
    }
    fn error(&self) {
        self.invoke(|o| o.error());
    }
    fn dropped(&self) {
        self.invoke(|o| o.dropped());
    }
    fn dropped_reason(&self, reason: PacketDropReason) {
        self.invoke(|o| o.dropped_reason(reason));
    }
}
impl Drop for Observer {
    fn drop(&mut self) {
        let original = self.original.take();
        // A last external observer Arc may execute user Drop too.
        self.broker.retire_observer(original);
    }
}
