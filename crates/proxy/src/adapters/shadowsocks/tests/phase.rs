//! Inspect the exact existing owners when a wire/ACK/cleanup wait is cancelled.
use std::sync::Arc;
pub(super) struct Phase {
    case: &'static str,
    current: &'static str,
    engine: zero_engine::Engine,
    originals: Vec<Arc<crate::runtime::inbound_owners::InboundOwner>>,
    peers: Vec<Arc<crate::runtime::inbound_owners::InboundPeerTasks>>,
}
impl Phase {
    pub(super) fn new(case: &'static str, proxy: &crate::Proxy) -> Self {
        Self {
            case,
            current: "cold ordinary configuration",
            engine: proxy.engine().clone(),
            originals: Vec::new(),
            peers: Vec::new(),
        }
    }
    pub(super) fn watch(&mut self, original: Arc<crate::runtime::inbound_owners::InboundOwner>) {
        self.originals.push(original);
    }
    pub(super) fn watch_peers(
        &mut self,
        original: Arc<crate::runtime::inbound_owners::InboundPeerTasks>,
    ) {
        self.peers.push(original);
    }
    pub(super) fn set(&mut self, current: &'static str) {
        self.current = current;
        self.inspect("enter");
    }
    fn inspect(&self, why: &str) {
        eprintln!(
            "SS actual budget phase {why}: {} / {}; actual active sessions={}",
            self.case,
            self.current,
            self.engine.active_sessions().len()
        );
        for peer in &self.peers {
            eprintln!("  same actual peer/native owner table: {peer:?}");
        }
        for original in &self.originals {
            eprintln!("  actual Source {:?}: running={} acquisition_idle={} accepted_join_consumed={} terminal={:?}",
                original.identity(), original.is_running(), original.acquisition_idle_for_test(), original.accepted_join_for_test().is_some(),
                original.terminal_for_test().map(|closed| (closed.joined_tasks(), closed.closed_io_receipts(), closed.failure().is_some())));
        }
    }
}
impl Drop for Phase {
    fn drop(&mut self) {
        // timeout drops its real work before unwrap panics; always inspect this
        // observer's final phase, without extending or replacing the deadline.
        self.inspect("last observed");
    }
}
