use super::{Control, ACTIVE, DRAIN};

#[derive(Debug, Clone, Copy)]
pub struct WorkerLoad {
    pub active: bool,
    pub connections: u32,
}

/// Called by the two-second bridge monitor after closed/drained workers leave
/// its admission set. The reference intentionally uses integer division.
pub fn needs_worker(workers: impl IntoIterator<Item = WorkerLoad>) -> bool {
    let mut count = 0u64;
    let mut connections = 0u64;
    for worker in workers {
        if worker.active {
            count += 1;
            connections += u64::from(worker.connections);
        }
    }
    count == 0 || connections / count > 16
}

#[derive(Debug, Default)]
pub struct BridgeState {
    state: i32,
    closed: bool,
}

impl BridgeState {
    pub fn apply(&mut self, control: &Control) {
        self.state = control.state;
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn active(&self) -> bool {
        !self.closed && self.state == ACTIVE
    }
}

#[derive(Debug, Default)]
pub struct PortalHeartbeat {
    counter: u8,
    draining: bool,
}

impl PortalHeartbeat {
    /// The caller invokes this every two seconds. Emit on the first tick and
    /// every fifth tick afterwards, or immediately when total sessions > 256.
    /// Total includes the MUX control session, matching the upstream worker.
    pub fn tick(&mut self, total: u32) -> Option<Control> {
        if self.draining {
            return None;
        }
        self.draining = total > 256;
        self.counter = (self.counter + 1) % 5;
        (self.draining || self.counter == 1)
            .then(|| Control::heartbeat(if self.draining { DRAIN } else { ACTIVE }))
    }
    pub fn draining(&self) -> bool {
        self.draining
    }
}
