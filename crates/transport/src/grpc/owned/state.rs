use super::*;
use crate::grpc::{pool::State, GrpcPool};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use zero_platform_tokio::{
    contract::{
        network::UpstreamTransport,
        scope::{TaskJoinWitness, TaskJoined, TaskRegistrar},
    },
    controlled_carrier::{CarrierAdoption, CarrierGrants},
};

#[derive(Default)]
pub(in crate::grpc) enum Ownership {
    #[default]
    Pristine,
    LegacyUsed,
    Managed(Arc<Managed>),
}
pub(in crate::grpc) struct Managed {
    pub(super) instance: ComponentInstanceId,
    pub(super) options: Arc<NativePoolOptions>,
    pub(super) upstream: UpstreamTransport,
    pub(super) tasks: TaskRegistrar,
    pub(super) cancel: CancellationToken,
    pub(super) scope_cancel: CancellationToken,
    pub(super) grants: CarrierGrants,
    pub(super) budget: Arc<super::budget::Budget>,
    pub(super) records: Mutex<Vec<Arc<Physical>>>,
    ready: AtomicBool,
}
pub(super) struct Physical {
    pub(super) adoption: CarrierAdoption,
    pub(super) witness: Mutex<Option<TaskJoinWitness>>,
    pub(super) received: Mutex<Option<TaskJoined>>,
    pub(super) accepted: AtomicBool,
    pub(super) done: AtomicBool,
}
pub(super) struct Body(pub(super) Arc<Physical>);
impl Drop for Body {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::Release);
    }
}
impl Physical {
    pub(super) fn closed(&self) -> bool {
        if !self.failure_accepted() {
            return false;
        }
        let Some(receipt) = self.adoption.closed() else {
            return false;
        };
        if receipt.instance() != self.adoption.instance() || !receipt.matches(&self.adoption) {
            return false;
        }
        match (self.witness.lock().unwrap().as_ref(), receipt.driver_exit()) {
            (None, None) => self.done.load(Ordering::Acquire),
            (Some(witness), Some(exit)) => witness
                .try_joined()
                .is_some_and(|joined| joined.id() == exit.id() && joined.class() == exit.class()),
            _ => false,
        }
    }
    pub(super) fn can_retire(&self) -> bool {
        self.receive_available();
        self.done.load(Ordering::Acquire)
            && self
                .witness
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|witness| witness.is_joined())
    }
}
impl GrpcPool {
    pub(in crate::grpc) fn enter_legacy(&self) -> Result<(), crate::RuntimeError> {
        let mut owner = self.0.ownership.lock().unwrap();
        match &*owner {
            Ownership::Pristine | Ownership::LegacyUsed => {
                *owner = Ownership::LegacyUsed;
                Ok(())
            }
            Ownership::Managed(_) => Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "gRPC pool belongs to its controlled native generation",
            )
            .into()),
        }
    }
}
impl State {
    pub(in crate::grpc) fn managed(
        &self,
        instance: ComponentInstanceId,
    ) -> Result<Arc<Managed>, ChannelError> {
        match &*self.ownership.lock().unwrap() {
            Ownership::Managed(owner)
                if owner.instance == instance
                    && owner.ready()
                    && !self.retired.load(Ordering::Acquire) =>
            {
                Ok(owner.clone())
            }
            _ => Err(error(instance, Stage::Prepare, Reason::Revoked)),
        }
    }
}
impl Managed {
    pub(super) fn new(
        instance: ComponentInstanceId,
        options: Arc<NativePoolOptions>,
        upstream: UpstreamTransport,
        tasks: TaskRegistrar,
        cancel: CancellationToken,
        grants: CarrierGrants,
    ) -> Arc<Self> {
        Arc::new(Self {
            instance,
            options: options.clone(),
            upstream,
            scope_cancel: tasks.cancellation(),
            tasks,
            cancel,
            grants,
            budget: super::budget::Budget::new(options.stream_limit),
            records: Mutex::new(Vec::new()),
            ready: AtomicBool::new(false),
        })
    }
    pub(super) fn activate(&self) -> Result<(), ChannelError> {
        if self.cancel.is_cancelled() || self.scope_cancel.is_cancelled() {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        self.ready.store(true, Ordering::Release);
        Ok(())
    }
    pub(super) fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
            && !self.cancel.is_cancelled()
            && !self.scope_cancel.is_cancelled()
    }
    pub(in crate::grpc) fn stop(&self) {
        self.ready.store(false, Ordering::Release);
        self.cancel.cancel();
    }
    pub(super) async fn reap(&self, deadline: Instant) -> Result<(), ChannelError> {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            if record.can_retire() {
                let _ = record.adoption.retire(deadline).await?;
            }
        }
        self.harvest();
        Ok(())
    }
    pub(super) fn harvest(&self) {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            record.receive_available();
        }
        self.records
            .lock()
            .unwrap()
            .retain(|record| !record.closed());
        self.budget.harvest();
    }
    pub(super) fn physical_unconfirmed(&self) -> bool {
        !self.records.lock().unwrap().is_empty()
    }
}
