//! Legacy Flow TX keeps its actual wire boundary, separate from Outer and TCP.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use zero_traits::{IoObserver, PacketDropReason};
#[derive(Debug)]
pub(crate) struct WireTraffic {
    parent: Option<Arc<dyn IoObserver>>,
    sent: AtomicUsize,
}
impl WireTraffic {
    pub(super) fn new(parent: Option<Arc<dyn IoObserver>>) -> Arc<Self> {
        Arc::new(Self {
            parent,
            sent: AtomicUsize::new(0),
        })
    }
    pub(crate) fn take_sent(&self) -> usize {
        self.sent.swap(0, Ordering::AcqRel)
    }
}
impl IoObserver for WireTraffic {
    fn stream_boundary(&self) {
        if let Some(parent) = &self.parent {
            parent.stream_boundary();
        }
    }
    fn datagram_boundary(&self) {
        if let Some(parent) = &self.parent {
            parent.datagram_boundary();
        }
    }
    fn receive_coverage_lost(&self) {
        if let Some(parent) = &self.parent {
            parent.receive_coverage_lost();
        }
    }
    fn sent_datagram(&self, bytes: usize) {
        self.sent.fetch_add(bytes, Ordering::Relaxed);
        if let Some(parent) = &self.parent {
            parent.sent_datagram(bytes);
        }
    }
    fn received_datagram(&self, bytes: usize) {
        if let Some(parent) = &self.parent {
            parent.received_datagram(bytes);
        }
    }
    fn sent(&self, bytes: usize) {
        if let Some(parent) = &self.parent {
            parent.sent(bytes);
        }
    }
    fn received(&self, bytes: usize) {
        if let Some(parent) = &self.parent {
            parent.received(bytes);
        }
    }
    fn error(&self) {
        if let Some(parent) = &self.parent {
            parent.error();
        }
    }
    fn dropped(&self) {
        if let Some(parent) = &self.parent {
            parent.dropped();
        }
    }
    fn dropped_reason(&self, reason: PacketDropReason) {
        if let Some(parent) = &self.parent {
            parent.dropped_reason(reason);
        }
    }
}
