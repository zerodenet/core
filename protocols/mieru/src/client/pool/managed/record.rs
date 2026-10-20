//! A slot survives future exit and real task join until physical closure is proven.
use super::ManagedState;
use std::sync::{Arc, Mutex};
use zero_platform_tokio::contract::scope::TaskJoinWitness;

pub(super) struct CarrierRecord(Mutex<Record>);
#[derive(Default)]
struct Record {
    attempted: bool,
    finished: bool,
    registering: bool,
    join: Option<TaskJoinWitness>,
}
pub(crate) struct CarrierLease {
    pub(crate) owner: Arc<ManagedState>,
    record: Arc<CarrierRecord>,
}
impl CarrierRecord {
    pub(super) fn new() -> Self {
        Self(Mutex::new(Record::default()))
    }
    pub(super) fn io_attempted(&self) -> bool {
        self.0.lock().unwrap().attempted
    }
    pub(super) fn body_finished(&self) -> bool {
        let record = self.0.lock().unwrap();
        record.finished && !record.registering
    }
    pub(super) async fn joined(&self) {
        let join = self.0.lock().unwrap().join.clone();
        if let Some(join) = join {
            let _ = join.wait().await;
        }
    }
}
impl CarrierLease {
    pub(super) fn new(owner: Arc<ManagedState>, record: Arc<CarrierRecord>) -> Self {
        Self { owner, record }
    }
    pub(crate) fn attempting_io(&self) {
        self.record.0.lock().unwrap().attempted = true;
    }
    pub(crate) fn registering(&self) -> Registration {
        self.record.0.lock().unwrap().registering = true;
        Registration {
            owner: self.owner.clone(),
            record: self.record.clone(),
        }
    }
}
pub(crate) struct Registration {
    owner: Arc<ManagedState>,
    record: Arc<CarrierRecord>,
}
impl Registration {
    pub(crate) fn registered(&self, witness: TaskJoinWitness) {
        self.record.0.lock().unwrap().join = Some(witness);
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.record.0.lock().unwrap().registering = false;
        self.owner.changed.notify_waiters();
    }
}
impl Drop for CarrierLease {
    fn drop(&mut self) {
        let attempted = {
            let mut record = self.record.0.lock().unwrap();
            record.finished = true;
            record.attempted
        };
        if !attempted {
            self.owner.release_unattempted(&self.record);
        }
        self.owner.changed.notify_waiters();
    }
}
