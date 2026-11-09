//! Existing worker wake and main's fault waiter; no task or cleanup executor.
use super::*;
use std::task::Waker;
pub(super) struct Wake {
    state: Mutex<State>,
}
struct State {
    changed: bool,
    waiter: Option<Waker>,
}
impl Wake {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                changed: false,
                waiter: None,
            }),
        })
    }
    pub(super) fn notify(&self) {
        let waiter = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.changed = true;
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
    /// Arm before checking queue/capacity. A concurrent actual change after
    /// this point sets the flag, preventing check-to-wait lost wakeups.
    pub(super) fn arm(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).changed = false;
    }
    pub(super) fn changed(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).changed
    }
    pub(super) fn register(&self, waker: &Waker) {
        let old = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.waiter.replace(waker.clone())
        };
        drop(old);
    }
    pub(super) fn clear(&self) {
        let old = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .waiter
            .take();
        drop(old);
    }
}

impl std::task::Wake for Wake {
    fn wake(self: Arc<Self>) {
        self.notify();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}
