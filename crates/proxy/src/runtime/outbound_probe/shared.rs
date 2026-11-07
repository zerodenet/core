//! The existing coalesced invocation retains its same actual host original.
use super::*;

pub(super) struct SharedProbeEntry {
    pub(super) future: SharedProbeFuture,
    pub(super) waiters: usize,
    pub(super) original: Arc<ProbeOriginal>,
}

/// Publication of the same accepted host call, never an executor or IO grant.
#[derive(Default)]
pub(super) struct ProbeOriginal {
    pub(super) cancelled: AtomicBool,
    pub(super) call: Mutex<Option<zero_runtime::ProbeCall>>,
}
impl ProbeOriginal {
    pub(super) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let call = self.call.lock().expect("probe original poisoned").clone();
        if let Some(call) = call {
            call.cancel();
        }
    }
    pub(super) fn install(&self, call: zero_runtime::ProbeCall) {
        *self.call.lock().expect("probe original poisoned") = Some(call.clone());
        if self.cancelled.load(Ordering::Acquire) {
            call.cancel();
        }
    }
}

pub(super) struct SharedProbeWaiter {
    pub(super) probes: Arc<Mutex<HashMap<ProbeKey, SharedProbeEntry>>>,
    pub(super) key: ProbeKey,
    pub(super) future: SharedProbeFuture,
    pub(super) original: Arc<ProbeOriginal>,
    pub(super) attached: bool,
}

impl Drop for SharedProbeWaiter {
    fn drop(&mut self) {
        self.detach();
    }
}
impl SharedProbeWaiter {
    pub(super) fn detach(&mut self) {
        if !self.attached {
            return;
        }
        self.attached = false;
        let mut probes = self
            .probes
            .lock()
            .expect("shared outbound probe lock poisoned");
        let Some(entry) = probes.get_mut(&self.key) else {
            drop(probes);
            self.original.cancel();
            return;
        };
        // Clearing a generation may have installed a new call with the same
        // public key. An old waiter must never decrement that new original.
        if !Arc::ptr_eq(&entry.original, &self.original) {
            drop(probes);
            self.original.cancel();
            return;
        }
        let removed = if entry.waiters <= 1 {
            probes.remove(&self.key)
        } else {
            entry.waiters -= 1;
            None
        };
        drop(probes);
        // A last waiter cancels a real host task. Never run its waker while
        // holding the shared-call map; the actual owner retains cleanup credit.
        if removed.is_some() {
            self.original.cancel();
        }
        drop(removed);
    }
}
