mod ownership;
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
    controlled_carrier::{CarrierAdoption, CarrierGrants, CarrierUserView},
};

#[derive(Default)]
pub(in crate::grpc) enum Ownership {
    #[default]
    Pristine,
    LegacyUsed,
    Managed(Arc<Managed>),
    Retired(super::retirement::Retired),
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
    pub(super) epoch: Mutex<Option<super::epoch::Epoch>>,
    pub(super) totals: Mutex<Option<super::totals::Active>>,
    #[cfg(feature = "tls")]
    pub(super) ech_resolver: Option<Arc<dyn crate::tls::ech::EchConfigResolver>>,
    #[cfg(feature = "tls")]
    pub(super) prepared_tls: Mutex<Option<crate::tls::OwnedPreparedClient>>,
}
pub(super) struct Physical {
    pub(super) pending: Mutex<Vec<CarrierAdoption>>,
    pub(super) adoption: CarrierAdoption,
    pub(super) stop: CancellationToken,
    pub(super) view: Mutex<Option<CarrierUserView>>,
    pub(super) witness: Mutex<Option<TaskJoinWitness>>,
    pub(super) received: Mutex<Option<TaskJoined>>,
    pub(super) accepted: AtomicBool,
    pub(super) done: AtomicBool,
}
pub(super) struct Body(pub(super) Arc<Physical>);
impl Drop for Body {
    fn drop(&mut self) {
        self.0.adoption.request_retirement();
        self.0.view.lock().unwrap().take();
        self.0.done.store(true, Ordering::Release);
    }
}
impl Physical {
    pub(super) fn closed(&self) -> bool {
        if !self.failure_accepted()
            || !self.pending.lock().unwrap().is_empty()
            || self
                .adoption
                .auxiliary_exit()
                .is_some_and(|joined| !joined.registration_released())
        {
            return false;
        }
        if !self
            .adoption
            .users_closed()
            .is_some_and(|users| users.matches(&self.adoption))
        {
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
            (Some(witness), Some(exit)) => witness.try_joined().is_some_and(|joined| {
                joined.id() == exit.id()
                    && joined.class() == exit.class()
                    && joined.registration_released()
            }),
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
        #[cfg(feature = "tls")] ech_resolver: Option<Arc<dyn crate::tls::ech::EchConfigResolver>>,
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
            epoch: Mutex::new(None),
            totals: Mutex::new(None),
            #[cfg(feature = "tls")]
            prepared_tls: Mutex::new(None),
            #[cfg(feature = "tls")]
            ech_resolver,
        })
    }
    pub(super) fn activate(&self) -> Result<(), ChannelError> {
        let totals = self
            .options
            .totals
            .as_ref()
            .map(|totals| totals.proposal.activate())
            .transpose()?;
        *self.totals.lock().unwrap() = totals;
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
            && self
                .totals
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|totals| totals.live())
    }
    pub(in crate::grpc) fn stop(&self) {
        self.ready.store(false, Ordering::Release);
        self.cancel.cancel();
    }
    pub(super) async fn reap_demand(&self, deadline: Option<Instant>) -> Result<(), ChannelError> {
        if let Some(end) = deadline {
            return self.reap(end).await;
        }
        let records = self.records.lock().unwrap().clone();
        for record in records {
            if record.can_retire() {
                record.release_installed_pending();
                let native = record.adoption.try_retire_native()?;
                if native
                    .closed()
                    .is_some_and(|closed| closed.matches(&record.adoption))
                {
                    // Native closure is already recorded. Full nonwaiting
                    // settlement still checks the same returned users/caller.
                    let _ = record.adoption.try_retire()?;
                }
                let pending = record.pending.lock().unwrap().clone();
                for original in pending {
                    let _ = original.try_retire()?;
                }
                record.release_installed_pending();
            }
        }
        self.harvest();
        Ok(())
    }
    pub(super) async fn reap(&self, deadline: Instant) -> Result<(), ChannelError> {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            if record.can_retire() {
                record.release_installed_pending();
                let _ = record.adoption.retire(deadline).await?;
                let pending = record.pending.lock().unwrap().clone();
                for original in pending {
                    let _ = original.retire(deadline).await?;
                }
                record.release_installed_pending();
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
    /// Positive old-generation settlement. Neither an expired Arc nor cancel
    /// alone permits a replacement executor for the same physical pool.
    pub(super) fn ready_to_replace(&self) -> bool {
        if !self.cancel.is_cancelled() {
            return false;
        }
        self.records
            .lock()
            .unwrap()
            .iter()
            .all(|record| record.closed())
            && self.budget.all_returned()
            && {
                let owner = self.grants.registration();
                owner.closed().is_some_and(|closed| closed.matches(&owner))
            }
    }
    pub(super) fn physical_unconfirmed(&self) -> bool {
        !self.records.lock().unwrap().is_empty()
    }
}
