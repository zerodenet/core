use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

pub(super) struct Record {
    state: Mutex<Registration>,
    changed: Notify,
    observed: AtomicBool,
    reported: AtomicBool,
}
enum Registration {
    Pending,
    Registered(TaskId, TaskJoinWitness),
    Rejected,
}
impl Record {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Registration::Pending),
            changed: Notify::new(),
            observed: AtomicBool::new(false),
            reported: AtomicBool::new(false),
        })
    }
    pub(super) fn observe_once(&self) -> bool {
        !self.observed.swap(true, Ordering::AcqRel)
    }
    pub(super) fn report_once(&self) -> bool {
        !self.reported.swap(true, Ordering::AcqRel)
    }
    pub(super) fn install(&self, id: TaskId, witness: TaskJoinWitness) {
        *self.state.lock().unwrap_or_else(|error| error.into_inner()) =
            Registration::Registered(id, witness);
        self.changed.notify_waiters();
    }
    pub(super) fn reject(&self) {
        *self.state.lock().unwrap_or_else(|error| error.into_inner()) = Registration::Rejected;
        self.changed.notify_waiters();
    }
    pub(super) fn registered(&self) -> Option<(TaskId, TaskJoinWitness)> {
        match &*self.state.lock().unwrap_or_else(|error| error.into_inner()) {
            Registration::Registered(id, witness) => Some((*id, witness.clone())),
            _ => None,
        }
    }
    pub(super) async fn wait_registration(&self) -> Option<(TaskId, TaskJoinWitness)> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match &*self.state.lock().unwrap_or_else(|error| error.into_inner()) {
                Registration::Registered(id, witness) => return Some((*id, witness.clone())),
                Registration::Rejected => return None,
                Registration::Pending => {}
            }
            changed.await;
        }
    }
}
