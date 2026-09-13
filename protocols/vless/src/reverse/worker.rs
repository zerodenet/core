use super::{Control, WorkerLoad, ACTIVE};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};

#[derive(Debug, Default)]
pub(crate) struct WorkerStatus {
    state: AtomicI32,
    connections: AtomicU32,
    closed: AtomicBool,
}
impl WorkerStatus {
    pub(crate) fn load(&self) -> WorkerLoad {
        WorkerLoad {
            active: !self.closed.load(Ordering::Acquire)
                && self.state.load(Ordering::Acquire) == ACTIVE,
            connections: self.connections.load(Ordering::Relaxed),
        }
    }
    pub(crate) fn control(&self, payload: &[u8]) -> Result<(), zero_core::Error> {
        self.state
            .store(Control::from_packet(payload)?.state, Ordering::Release);
        Ok(())
    }
    pub(crate) fn connections(&self, count: usize) {
        self.connections
            .store(count.min(u32::MAX as usize) as u32, Ordering::Relaxed);
    }
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}
