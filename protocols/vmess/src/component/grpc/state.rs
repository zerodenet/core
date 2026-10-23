use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use zero_platform_tokio::{
    contract::{
        admission::SessionAdmission,
        network::UpstreamTransport,
        scope::{TaskJoinWitness, TaskJoined, TaskRegistrar},
        CancellationToken,
    },
    controlled_carrier::{CarrierAdoption, CarrierGrants},
};
pub(super) struct Managed {
    pub(super) instance: ComponentInstanceId,
    pub(super) options: Arc<Options>,
    pub(super) admission: Arc<SessionAdmission>,
    pub(super) prepared: super::super::owner::Prepared,
    pub(super) upstream: Option<UpstreamTransport>,
    pub(super) tasks: TaskRegistrar,
    pub(super) cancel: CancellationToken,
    pub(super) ready: AtomicBool,
    pub(super) grants: CarrierGrants,
    pub(super) records: Mutex<Vec<Arc<Record>>>,
}
pub(super) struct Record {
    pub(super) adoption: CarrierAdoption,
    pub(super) witness: Mutex<Option<TaskJoinWitness>>,
    pub(super) received: Mutex<Option<TaskJoined>>,
    pub(super) accepted: AtomicBool,
    pub(super) stop: CancellationToken,
    pub(super) opening: AtomicBool,
    pub(super) done: AtomicBool,
    // Actual logical view returned to the caller, distinct from driver body exit.
    pub(super) user_live: AtomicBool,
}
pub(super) struct Opening(pub(super) Arc<Record>, pub(super) std::sync::Weak<Managed>);
impl Drop for Opening {
    fn drop(&mut self) {
        self.0.opening.store(false, Ordering::Release);
        if let Some(owner) = self.1.upgrade() {
            // A rejected unused grant has a private no-acquisition receipt.
            // Reclaim that completed metadata even under concurrent rejections.
            owner
                .records
                .lock()
                .unwrap()
                .retain(|record| !record.closed());
        }
    }
}
pub(super) struct Body(pub(super) Arc<Record>);
impl Drop for Body {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::Release);
    }
}
impl Record {
    pub(super) fn closed(&self) -> bool {
        if self.user_live.load(Ordering::Acquire) || !self.failure_accepted() {
            return false;
        }
        let Some(closed) = self.adoption.closed() else {
            return false;
        };
        if !closed.matches(&self.adoption) || closed.instance() != self.adoption.instance() {
            return false;
        }
        match (self.witness.lock().unwrap().as_ref(), closed.driver_exit()) {
            (None, None) => !self.opening.load(Ordering::Acquire),
            (Some(witness), Some(exit)) => witness
                .try_joined()
                .is_some_and(|joined| joined.id() == exit.id() && joined.class() == exit.class()),
            _ => false,
        }
    }
}
impl Managed {
    pub(super) fn check(&self) -> Result<(), ChannelError> {
        if !self.ready.load(Ordering::Acquire)
            || self.cancel.is_cancelled()
            || self.tasks.cancellation().is_cancelled()
            || !self.options.state.current(self.instance)
        {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        Ok(())
    }
    pub(super) fn stop(&self) {
        self.ready.store(false, Ordering::Release);
        self.cancel.cancel();
        let records = self.records.lock().unwrap().clone();
        for record in records {
            record.stop.cancel();
        }
    }
    pub(super) async fn reap(&self, deadline: Instant) -> Result<(), ChannelError> {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            record.receive_available();
            let (joined, has_driver) = {
                let witness = record.witness.lock().unwrap();
                (
                    witness.as_ref().is_none_or(TaskJoinWitness::is_joined),
                    witness.is_some(),
                )
            };
            if !record.opening.load(Ordering::Acquire)
                && !record.user_live.load(Ordering::Acquire)
                && joined
                && (record.done.load(Ordering::Acquire) || !has_driver)
            {
                let report = record.adoption.retire(deadline).await?;
                if !report.is_complete() {
                    let mut failure = error(self.instance, Stage::Close, Reason::Timeout);
                    failure.delivery = DeliveryState::Unknown;
                    return Err(failure);
                }
                // After checking the matching private receipt below, discard
                // completed metadata rather than accumulating a history per dial.
            }
        }
        self.records
            .lock()
            .unwrap()
            .retain(|record| !record.closed());
        Ok(())
    }
}
