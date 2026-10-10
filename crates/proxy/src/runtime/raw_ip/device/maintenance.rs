//! Resource cleanup is independent of protocol timing. Lookup/admission paths
//! enforce expiry, so reclaiming bounded tables need not scan every 250 ms.
use std::time::{Duration, Instant};

pub(super) struct ReturnMaintenance {
    next: Instant,
}
impl ReturnMaintenance {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            next: now + Duration::from_secs(5),
        }
    }
    pub(super) fn due(&mut self, now: Instant) -> bool {
        if now < self.next {
            return false;
        }
        // No cleanup burst after a suspended or congested runtime.
        self.next = now + Duration::from_secs(5);
        true
    }
}

#[cfg(test)]
mod tests;
