//! Legacy Flow TX keeps its actual wire boundary, separate from Outer and TCP.
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use zero_traits::{IoObserver, PacketDropReason};
#[derive(Debug)]
pub(crate) struct WireTraffic {
    parent: Option<Arc<dyn IoObserver>>,
    sent: AtomicUsize,
    reported: AtomicBool,
}
impl WireTraffic {
    pub(in crate::component::stream_bridge) fn new(
        parent: Option<Arc<dyn IoObserver>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            parent,
            sent: AtomicUsize::new(0),
            reported: AtomicBool::new(false),
        })
    }
    pub(crate) fn take_flow_sent(&self) -> Option<usize> {
        let reported = self.reported.swap(false, Ordering::AcqRel);
        let bytes = self.take_sent();
        reported.then_some(bytes)
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
        self.reported.store(true, Ordering::Release);
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

impl zero_platform_tokio::contract::context::ConnectObserver for WireTraffic {
    fn record(
        &self,
        _: Option<zero_core::component::SessionId>,
        _: zero_platform_tokio::contract::context::ConnectFact,
    ) {
    }
    fn datagram_sent(&self, _: Option<zero_core::component::SessionId>, encoded_bytes: u64) {
        // The TCP carrier already reports physical Outer bytes to its observer.
        self.reported.store(true, Ordering::Release);
        self.sent
            .fetch_add(encoded_bytes as usize, Ordering::Relaxed);
    }
}
