//! Same accept consumer's wake, deferred only past its actual owner locks.
//! No task, extra queue, timer or completion/emptiness evidence is introduced.
use std::{
    sync::{Arc, Mutex},
    task::{Wake, Waker},
};
#[derive(Clone, Default)]
pub(super) struct AcceptWake(Arc<Inner>);
#[derive(Default)]
struct Inner(Mutex<State>);
#[derive(Default)]
struct State {
    deferred: usize,
    requested: bool,
    target: Option<Arc<Waker>>,
}
pub(super) struct Notifications(Arc<Inner>);
impl AcceptWake {
    pub(super) fn register(&self, target: &Waker) {
        // RawWaker clone/drop can invoke external code; neither happens under
        // the queue, connection or this relay's own bookkeeping lock.
        let target = Arc::new(target.clone());
        let old = self.0 .0.lock().unwrap().target.replace(target);
        drop(old);
    }
    pub(super) fn waker(&self) -> Waker {
        Waker::from(self.0.clone())
    }
    pub(super) fn defer(&self) -> Notifications {
        let mut state = self.0 .0.lock().unwrap();
        state.deferred = state
            .deferred
            .checked_add(1)
            .expect("accept wake guard overflow");
        Notifications(self.0.clone())
    }
}
impl Inner {
    fn request(&self) {
        let target = {
            let mut state = self.0.lock().unwrap();
            state.requested = true;
            if state.deferred == 0 {
                state.requested = false;
                state.target.clone()
            } else {
                None
            }
        };
        if let Some(target) = target {
            target.wake_by_ref();
        }
    }
}
impl Wake for Inner {
    fn wake(self: Arc<Self>) {
        self.request();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.request();
    }
}
impl Drop for Notifications {
    fn drop(&mut self) {
        let target = {
            let mut state = self.0 .0.lock().unwrap();
            state.deferred -= 1;
            if state.deferred == 0 && state.requested {
                state.requested = false;
                state.target.clone()
            } else {
                None
            }
        };
        if let Some(target) = target {
            target.wake_by_ref();
        }
    }
}
