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
    pub(super) ech_resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    pub(super) upstream: Option<UpstreamTransport>,
    pub(super) tasks: TaskRegistrar,
    pub(super) cancel: CancellationToken,
    pub(super) ready: AtomicBool,
    pub(super) grants: CarrierGrants,
    pub(super) service_grants: Option<CarrierGrants>,
    pub(super) records: Mutex<Vec<Arc<Record>>>,
    pub(super) native_budget: Arc<tokio::sync::Semaphore>,
    pub(super) network: tokio::sync::Mutex<
        Option<(
            zero_core::component::NetworkGeneration,
            Option<zero_platform_tokio::contract::dns::DnsGeneration>,
        )>,
    >,
}
pub(super) struct Record {
    pub(super) adoption: CarrierAdoption,
    pub(super) capacity: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    pub(super) witness: Mutex<Option<TaskJoinWitness>>,
    pub(super) exit_changed: tokio::sync::Notify,
    pub(super) received: Mutex<Option<TaskJoined>>,
    pub(super) accepted: AtomicBool,
    pub(super) stop: CancellationToken,
    pub(super) opening: AtomicBool,
    pub(super) published: AtomicBool,
    pub(super) done: AtomicBool,
    // Actual logical view returned to the caller, distinct from driver body exit.
    pub(super) user_live: AtomicBool,
    pub(super) tracked_view: bool,
}
pub(super) struct Opening(pub(super) Arc<Record>, pub(super) std::sync::Weak<Managed>);
impl Drop for Opening {
    fn drop(&mut self) {
        self.0.opening.store(false, Ordering::Release);
        self.0.exit_changed.notify_waiters();
        if !self.0.published.load(Ordering::Acquire) {
            self.0.stop.cancel();
            self.0.adoption.request_retirement();
        }
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
    pub(super) async fn retire(
        &self,
        deadline: Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError> {
        let witness = self.witness.lock().unwrap().clone();
        if let Some(witness) = witness {
            let joined = match tokio::time::timeout_at(deadline, witness.wait()).await {
                Ok(joined) => joined,
                Err(_) => {
                    return Ok(zero_platform_tokio::contract::lifecycle::CloseReport {
                        unfinished: vec![
                            zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                                "VMess original gRPC driver join remains pending".into(),
                            ),
                        ],
                        ..Default::default()
                    })
                }
            };
            self.receive(joined);
        }
        self.adoption.retire(deadline).await
    }
    pub(super) fn closed(&self) -> bool {
        if self.user_live.load(Ordering::Acquire) || !self.failure_accepted() {
            return false;
        }
        if self.tracked_view
            && !self
                .adoption
                .users_closed()
                .is_some_and(|closed| closed.matches(&self.adoption))
        {
            return false;
        }
        let Some(closed) = self.adoption.closed() else {
            return false;
        };
        if !closed.matches(&self.adoption) || closed.instance() != self.adoption.instance() {
            return false;
        }
        let complete = match (self.witness.lock().unwrap().as_ref(), closed.driver_exit()) {
            (None, None) => !self.opening.load(Ordering::Acquire),
            (Some(witness), Some(exit)) => witness
                .try_joined()
                .is_some_and(|joined| joined.id() == exit.id() && joined.class() == exit.class()),
            _ => false,
        };
        if complete {
            // No historical control or receipt handle may retain admission.
            // This is reached only after the actual original, view and driver
            // matched above and its original task failure was accepted.
            self.capacity.lock().unwrap().take();
        }
        complete
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
            if self.options.node()
                && !record.opening.load(Ordering::Acquire)
                && !record.user_live.load(Ordering::Acquire)
            {
                let report = record.retire(deadline).await?;
                if !report.is_complete() {
                    let mut failure = error(self.instance, Stage::Close, Reason::Timeout);
                    failure.delivery = DeliveryState::Unknown;
                    return Err(failure);
                }
                continue;
            }
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
