//! A rejected original's finite noncached job, distinct from normal delivery.
use super::*;
use crate::contract::scope::{TaskJoined, TaskRegistrar};
use crate::controlled_carrier::totals::preparation::{
    CarrierPreparationRequest, PreparationBudget,
};
use std::sync::Mutex;
pub(super) struct StagedWork {
    delay: std::time::Duration,
    work: crate::contract::BoxFuture<'static, Result<(), ChannelError>>,
}
pub struct StagedCarrierPreparationJob {
    pub grant: CarrierPreparationJob,
    pub return_delay: std::time::Duration,
    pub work: crate::contract::BoxFuture<'static, Result<(), ChannelError>>,
}
pub(super) mod exit;
mod failure;
mod retained;
pub(super) mod staged;
pub use retained::CarrierPreparationJobRetained;

pub(in crate::controlled_carrier) struct Job {
    budget: PreparationBudget,
    scope: crate::contract::scope::TaskScopeIdentity,
    failure: Mutex<Option<(Arc<ChannelError>, TaskJoined)>>,
    install_failure: OnceLock<Arc<ChannelError>>,
    execution_exit: OnceLock<TaskJoined>,
    driver: OnceLock<TaskJoinWitness>,
    sealed: AtomicBool,
    staged: Mutex<Option<StagedWork>>,
}
/// A grant only for this actual original's preparation job accounting. It has
/// no native acquisition, root-origin, cached ownership or Caller binding API.
#[derive(Clone)]
pub struct CarrierPreparationJob {
    slot: Weak<Slot>,
    preparation: Arc<Preparation>,
    job: Arc<Job>,
}
impl CarrierPreparation {
    /// Called by the real waiting original owner while its actual setup Aux
    /// still owns the acquired IO. Root admission precedes storing the job.
    pub fn reserve_job(&self) -> Result<CarrierPreparationJob, ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(revoked)?;
        let fail = || {
            error(
                slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            )
        };
        let owner = slot.owner.upgrade().ok_or_else(fail)?;
        let bound = owner.state().binding.clone().ok_or_else(fail)?;
        let actual = self.state.auxiliary.get().ok_or_else(fail)?;
        if !bound.observes_preparation_job(actual)
            || actual.is_joined()
            || !actual.scope_identity().resource_open()
            || self.state.published.load(Ordering::Acquire)
            || slot.cancel.is_cancelled()
        {
            return Err(fail());
        }
        let _closing = slot.closing.try_lock().map_err(|_| fail())?;
        let state = slot.state();
        if state.closed
            || state.spawning
            || state.spawn_attempted
            || !state.view_live
            || state.unknown.is_some()
            || !state.users.as_ref().is_some_and(|users| {
                users.live()
                    && users.acquired()
                    && users
                        .preparation
                        .as_ref()
                        .is_some_and(|p| Arc::ptr_eq(p, &self.state))
            })
        {
            return Err(fail());
        }
        let root = owner.totals.get().ok_or_else(fail)?;
        let budget = root.reserve_preparation(&slot)?;
        let job = Arc::new(Job {
            budget,
            scope: actual.scope_identity(),
            failure: Mutex::new(None),
            install_failure: OnceLock::new(),
            execution_exit: OnceLock::new(),
            driver: OnceLock::new(),
            sealed: AtomicBool::new(false),
            staged: Mutex::new(None),
        });
        self.state.job.set(job.clone()).map_err(|_| fail())?;
        drop(state);
        Ok(CarrierPreparationJob {
            slot: Arc::downgrade(&slot),
            preparation: self.state.clone(),
            job,
        })
    }
    pub fn job_retained(&self) -> Option<CarrierPreparationJobRetained> {
        let slot = self.slot.upgrade()?;
        let job = self.state.job.get()?;
        job.sealed
            .load(Ordering::Acquire)
            .then(|| CarrierPreparationJobRetained {
                slot: Arc::downgrade(&slot),
                job: job.clone(),
            })
    }
}
fn revoked() -> ChannelError {
    ChannelError::new(
        Stage::Prepare,
        Reason::Revoked,
        crate::contract::Impact::Component,
    )
}
impl CarrierPreparationJob {
    pub(in crate::controlled_carrier) fn matches_slot(
        &self,
        slot: &Arc<Slot>,
        tasks: &TaskRegistrar,
    ) -> bool {
        self.slot
            .upgrade()
            .is_some_and(|actual| Arc::ptr_eq(&actual, slot))
            && self.job.scope.matches(tasks)
            && self
                .preparation
                .job
                .get()
                .is_some_and(|actual| Arc::ptr_eq(actual, &self.job))
    }
    /// Store the IO-owning future on this exact original before returning the
    /// rejected preparation. It cannot poll here. Cancellation removes this
    /// same finite cell before native cleanup, breaking its custody cycle.
    pub fn stage(
        &self,
        return_delay: std::time::Duration,
        work: crate::contract::BoxFuture<'static, Result<(), ChannelError>>,
    ) -> Result<(), ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(revoked)?;
        if slot.cancel.is_cancelled()
            || self.job.sealed.load(Ordering::Acquire)
            || self
                .preparation
                .auxiliary
                .get()
                .is_none_or(TaskJoinWitness::is_joined)
            || self.preparation.published.load(Ordering::Acquire)
        {
            return Err(revoked());
        }
        let mut staged = self.job.staged.lock().unwrap();
        // Serialize the check with the cancelled-slot take. A cancelling
        // Root either takes this exact future, or staging sees its fence.
        if staged.is_some() || slot.cancel.is_cancelled() {
            return Err(revoked());
        }
        *staged = Some(StagedWork {
            delay: return_delay,
            work,
        });
        Ok(())
    }
    pub(crate) fn ready_for_spawn(&self) -> bool {
        self.job.failure.lock().unwrap().is_some() && !self.job.sealed.load(Ordering::Acquire)
    }
    pub fn matches_preparation(&self, original: &CarrierPreparation) -> bool {
        Arc::ptr_eq(&self.preparation, &original.state)
            && original
                .state
                .job
                .get()
                .is_some_and(|actual| Arc::ptr_eq(actual, &self.job))
            && self
                .slot
                .upgrade()
                .zip(original.slot.upgrade())
                .is_some_and(|(left, right)| Arc::ptr_eq(&left, &right))
            && !original.is_published()
            && original
                .state
                .auxiliary
                .get()
                .is_some_and(|actual| !actual.is_joined())
    }
    pub(crate) fn set_driver(&self, witness: TaskJoinWitness) -> Result<(), ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(revoked)?;
        if !witness.scope_identity().same_scope(&self.job.scope)
            || witness.is_joined()
            || self.job.failure.lock().unwrap().is_none()
            || self.job.driver.set(witness).is_err()
        {
            return Err(error(
                slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        Ok(())
    }
    /// Seal only after the actual original driver registration owns the sole IO
    /// future. No native, user, Scope or ACK receipt is issued by this handoff.
    pub(crate) fn seal(&self) -> Result<(), ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(revoked)?;
        let fail = || {
            error(
                slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            )
        };
        let state = slot.state();
        let driver = self.job.driver.get().ok_or_else(fail)?;
        if slot.cancel.is_cancelled()
            || !self.job.scope.resource_open()
            || state
                .business_cancel
                .as_ref()
                .is_some_and(crate::contract::CancellationToken::is_cancelled)
            || state.closed
            || !state
                .driver
                .as_ref()
                .is_some_and(|(_, actual)| actual.same_registration(driver))
            || self.preparation.published.load(Ordering::Acquire)
            || self.job.failure.lock().unwrap().is_none()
            || self.job.staged.lock().unwrap().is_some()
            || self.job.sealed.swap(true, Ordering::AcqRel)
        {
            return Err(fail());
        }
        drop(state);
        slot.notify();
        Ok(())
    }
    pub async fn request(&self) -> Result<CarrierPreparationRequest, ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(revoked)?;
        if !self.job.sealed.load(Ordering::Acquire)
            || slot.cancel.is_cancelled()
            || !self.job.scope.resource_open()
            || self.job.driver.get().is_none_or(TaskJoinWitness::is_joined)
        {
            return Err(revoked());
        }
        self.job.budget.request().await
    }
    pub fn cancel(&self) {
        if let Some(slot) = self.slot.upgrade() {
            crate::controlled_carrier::CarrierAdoption { slot }.request_retirement();
        }
    }
}
