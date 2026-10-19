//! Reuse the socket observer boundary for establishment-byte accounting.
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use zero_traits::IoObserver;

#[derive(Debug)]
pub(super) struct EstablishmentTraffic {
    parent: Option<Arc<dyn IoObserver>>,
    active: AtomicBool,
    read: AtomicU64,
    written: AtomicU64,
}
impl EstablishmentTraffic {
    pub(super) fn new(parent: Option<Arc<dyn IoObserver>>) -> Arc<Self> {
        Arc::new(Self {
            parent,
            active: AtomicBool::new(true),
            read: AtomicU64::new(0),
            written: AtomicU64::new(0),
        })
    }
    pub(super) fn finish(&self) -> zero_transport::StreamTraffic {
        self.active.store(false, Ordering::Release);
        zero_transport::StreamTraffic {
            read_bytes: self.read.swap(0, Ordering::Relaxed),
            written_bytes: self.written.swap(0, Ordering::Relaxed),
        }
    }
}
impl IoObserver for EstablishmentTraffic {
    fn stream_boundary(&self) {
        if let Some(parent) = &self.parent {
            parent.stream_boundary();
        }
    }
    fn received(&self, bytes: usize) {
        if self.active.load(Ordering::Acquire) {
            self.read.fetch_add(bytes as u64, Ordering::Relaxed);
        }
        if let Some(parent) = &self.parent {
            parent.received(bytes);
        }
    }
    fn sent(&self, bytes: usize) {
        if self.active.load(Ordering::Acquire) {
            self.written.fetch_add(bytes as u64, Ordering::Relaxed);
        }
        if let Some(parent) = &self.parent {
            parent.sent(bytes);
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
}
