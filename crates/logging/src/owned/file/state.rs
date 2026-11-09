use super::*;
impl Entry {
    pub(in crate::owned) fn stopping(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(in crate::owned) fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        self.gate.open();
        self.book.wake.notify();
    }
    pub(in crate::owned) fn initialized(&self) -> bool {
        self.phase.load(std::sync::atomic::Ordering::Acquire) == 1
    }
    pub(in crate::owned) fn failed_initialization(&self) -> bool {
        self.phase.load(std::sync::atomic::Ordering::Acquire) == 2
    }
    pub(in crate::owned) fn mark_ready(&self) {
        self.phase.store(1, std::sync::atomic::Ordering::Release);
        self.book.wake.notify();
    }
    pub(in crate::owned) fn mark_initialization_failed(&self) {
        self.phase.store(2, std::sync::atomic::Ordering::Release);
        self.book.wake.notify();
    }
    pub(in crate::owned) fn has_pending_io(&self) -> bool {
        self.faults
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending
            .iter()
            .any(Option::is_some)
    }
    pub(in crate::owned) fn is_unknown(&self) -> bool {
        self.unknown.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(in crate::owned) fn peek_failure(&self) -> Option<fault::IoFailure> {
        self.faults
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending
            .iter()
            .find_map(Clone::clone)
    }
    pub(in crate::owned) fn accept_failure(&self, source: &fault::IoFailure) -> bool {
        if !source.matches_book(&self.book) || source.output() != self.output {
            return false;
        }
        let old = {
            let mut faults = self.faults.lock().unwrap_or_else(|e| e.into_inner());
            let Some(slot) = faults
                .pending
                .iter_mut()
                .find(|slot| slot.as_ref().is_some_and(|actual| actual.same(source)))
            else {
                return source.accepted();
            };
            source.accept();
            slot.take()
        };
        drop(old);
        self.book.wake.notify();
        true
    }

    pub(in crate::owned) fn native_closed(&self) -> bool {
        self.native_closed
            .load(std::sync::atomic::Ordering::Acquire)
    }
    pub(in crate::owned) fn needs_reopen(&self) -> bool {
        self.reopen.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(in crate::owned) fn queue_empty(&self) -> bool {
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }
    pub(in crate::owned) fn require_reopen(&self) {
        self.reopen
            .store(true, std::sync::atomic::Ordering::Release);
    }
    /// The replacement actual worker is registered only after its predecessor
    /// joined and released registration. Close/open remain on THAT worker.
    /// No new IO until the same old controller has a matching close receipt.
    pub(in crate::owned) fn recover_closed(&self) -> bool {
        if !self.reopen.load(std::sync::atomic::Ordering::Acquire) {
            return true;
        }
        if self.has_pending_io() || !self.ensure_slot(2) || !self.close() {
            return false;
        }
        let previous = {
            let mut native = self.native.lock().unwrap_or_else(|e| e.into_inner());
            if native.acquiring || native.unknown.is_some() {
                self.unknown
                    .store(true, std::sync::atomic::Ordering::Release);
                return false;
            }
            if native.original.as_ref().is_some_and(|original| {
                !native
                    .closed
                    .as_ref()
                    .is_some_and(|closed| original.matches_closed(closed))
            }) {
                return false;
            }
            native.written = 0;
            native.rotating = false;
            native.closed = None;
            native.original.take()
        };
        drop(previous);
        self.phase.store(0, std::sync::atomic::Ordering::Release);
        self.reopen
            .store(false, std::sync::atomic::Ordering::Release);
        true
    }
}
