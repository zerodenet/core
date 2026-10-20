//! A slot survives future exit and real task join until physical closure is proven.
use super::ManagedState;
use std::sync::{Arc, Mutex};
use zero_platform_tokio::contract::scope::TaskJoinWitness;
use zero_platform_tokio::controlled_carrier::CarrierAdoption;

pub(super) struct CarrierRecord(Mutex<Record>);
#[derive(Default)]
struct Record {
    attempted: bool,
    finished: bool,
    registering: bool,
    join: Option<TaskJoinWitness>,
    adoption: Option<CarrierAdoption>,
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
    pub(super) fn adoption(&self) -> Option<CarrierAdoption> {
        self.0.lock().unwrap().adoption.clone()
    }
    pub(super) fn closed(&self, instance: zero_core::component::ComponentInstanceId) -> bool {
        let record = self.0.lock().unwrap();
        if !record.finished || record.registering {
            return false;
        }
        let Some(adoption) = &record.adoption else {
            return false;
        };
        let Some(receipt) = adoption.closed() else {
            return false;
        };
        if receipt.instance() != instance || !receipt.matches(adoption) {
            return false;
        }
        match (&record.join, receipt.driver_exit()) {
            (None, None) => true,
            (Some(witness), Some(exit)) => witness
                .try_joined()
                .is_some_and(|joined| joined.id() == exit.id() && joined.class() == exit.class()),
            _ => false,
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
    pub(crate) fn adopt(
        &self,
        adoption: CarrierAdoption,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        if adoption.instance() != self.owner.instance {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::StalePlan,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        let mut record = self.record.0.lock().unwrap();
        if record.adoption.is_some() || record.attempted || record.registering || record.finished {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        record.adoption = Some(adoption);
        Ok(())
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
