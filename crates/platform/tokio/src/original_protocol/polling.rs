//! Unwinding cannot permanently fence this original's standing continuation.
use super::*;
pub(super) struct PollingReset<'a> {
    phase: &'a Phase,
    armed: bool,
}
impl<'a> PollingReset<'a> {
    pub(super) fn new(phase: &'a Phase) -> Self {
        Self { phase, armed: true }
    }
    pub(super) fn finish(&mut self) {
        self.armed = false;
    }
}
impl Drop for PollingReset<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let woken = {
            let mut state = self
                .phase
                .operation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.polling = false;
            std::mem::take(&mut state.woken)
        };
        // Never invoke arbitrary outer waker code while already unwinding.
        // The original control/Root reservation stays strong; no close or ACK
        // is inferred from unwinding or from discarding a poisoned future.
        if woken && !std::thread::panicking() {
            self.phase.wake_now();
        }
    }
}
