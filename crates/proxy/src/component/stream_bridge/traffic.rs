//! Reuse the socket observer boundary for establishment-byte accounting.
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use zero_core::component::SessionId;
use zero_platform_tokio::contract::context::{ConnectFact, ConnectObserver};
use zero_traits::IoObserver;

#[derive(Debug)]
pub(super) struct EstablishmentTraffic {
    parent: Option<Arc<dyn IoObserver>>,
    active: AtomicBool,
    read: AtomicU64,
    written: AtomicU64,
    protocol: Mutex<Option<zero_transport::StreamTraffic>>,
}
impl EstablishmentTraffic {
    pub(super) fn new(parent: Option<Arc<dyn IoObserver>>) -> Arc<Self> {
        Arc::new(Self {
            parent,
            active: AtomicBool::new(true),
            read: AtomicU64::new(0),
            written: AtomicU64::new(0),
            protocol: Mutex::new(None),
        })
    }
    pub(super) fn finish(&self) -> zero_transport::StreamTraffic {
        self.active.store(false, Ordering::Release);
        let carrier = zero_transport::StreamTraffic {
            read_bytes: self.read.swap(0, Ordering::Relaxed),
            written_bytes: self.written.swap(0, Ordering::Relaxed),
        };
        self.protocol
            .lock()
            .expect("protocol traffic poisoned")
            .take()
            .unwrap_or(carrier)
    }
}
impl ConnectObserver for EstablishmentTraffic {
    fn record(&self, _: Option<SessionId>, _: ConnectFact) {}
    fn protocol_traffic(&self, _: Option<SessionId>, read: u64, written: u64) {
        // It is one complete result, not a delta; repeating it cannot double
        // charge Flow. Physical Outer observations still reach the parent.
        *self.protocol.lock().expect("protocol traffic poisoned") =
            Some(zero_transport::StreamTraffic {
                read_bytes: read,
                written_bytes: written,
            });
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

#[cfg(test)]
#[path = "../tests/stream_traffic.rs"]
mod tests;
