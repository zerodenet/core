//! Exact listener attempts, not a counter which can outlive its original.
use super::*;
use crate::{
    contract::{
        scope::{ScopeRetirementObservation, TaskRegistrar},
        BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    controlled_carrier::{CarrierOriginalTotals, SourceCustody},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Weak,
    },
    task::Context,
};
use zero_core::component::ComponentInstanceId;

#[derive(Clone)]
pub(super) struct Binding {
    pub(super) instance: ComponentInstanceId,
    pub(super) root: Weak<CarrierOriginalTotals>,
    pub(super) scope: ScopeRetirementObservation,
}
pub(super) struct Attempt {
    acquired: AtomicBool,
    native_closed: AtomicBool,
    failed_return: AtomicBool,
    published: AtomicBool,
    view_returned: AtomicBool,
    changed: tokio::sync::Notify,
}
impl Attempt {
    pub(super) fn new() -> Self {
        Self {
            acquired: AtomicBool::new(false),
            native_closed: AtomicBool::new(false),
            failed_return: AtomicBool::new(false),
            published: AtomicBool::new(false),
            view_returned: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        }
    }
    pub(super) fn acquired(&self) {
        self.acquired.store(true, Ordering::Release);
    }
    pub(super) fn native_closed(&self) {
        self.native_closed.store(true, Ordering::Release);
    }
    pub(super) fn failed_return(&self) {
        self.failed_return.store(true, Ordering::Release);
    }
    pub(super) fn publish_view(&self) {
        self.published.store(true, Ordering::Release);
    }
    pub(super) fn return_view(&self) {
        self.view_returned.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    fn native_complete(&self) -> bool {
        self.native_closed.load(Ordering::Acquire)
            || (self.failed_return.load(Ordering::Acquire)
                && !self.acquired.load(Ordering::Acquire))
    }
}
impl Binding {
    pub(super) fn reserve(
        &self,
        owner: &Arc<Shared>,
        attempt: &Arc<Attempt>,
    ) -> Result<Reservation, ChannelError> {
        let root = self
            .root
            .upgrade()
            .ok_or_else(|| error(self.instance, Reason::Revoked))?;
        let original: Arc<dyn SourceCustody> = Arc::new(Original {
            owner: owner.clone(),
            attempt: attempt.clone(),
            scope: self.scope.clone(),
            waiting: Mutex::default(),
            polling: AtomicBool::new(false),
            repoll: AtomicBool::new(false),
            first_fault: Mutex::default(),
            native: Mutex::default(),
            native_finished: AtomicBool::new(false),
            users_finished: AtomicBool::new(false),
            deadline: std::sync::OnceLock::new(),
        });
        root.reserve_source(self.instance, original.clone())?;
        Ok(Reservation { root, original })
    }
}
impl ControlledTcpListenerOwner {
    /// The real hosting constructor calls this on the same cold native owner.
    /// No arbitrary identity-to-registrar conversion or native IO is performed.
    pub fn bind_original_totals(
        &mut self,
        instance: ComponentInstanceId,
        tasks: &TaskRegistrar,
        root: &Arc<CarrierOriginalTotals>,
    ) -> Result<(), ChannelError> {
        self.bind_original_totals_observed(instance, tasks.retirement_observation(), root)
    }
    /// Same actual weak-issued registrar observation, never identity conversion.
    pub fn bind_original_totals_observed(
        &mut self,
        instance: ComponentInstanceId,
        scope: ScopeRetirementObservation,
        root: &Arc<CarrierOriginalTotals>,
    ) -> Result<(), ChannelError> {
        let shared = &self.control.shared;
        if let Some(actual) = shared.originals.get() {
            return if actual.instance == instance
                && actual.root.ptr_eq(&Arc::downgrade(root))
                && actual
                    .scope
                    .scope_identity()
                    .same_scope(&scope.scope_identity())
            {
                Ok(())
            } else {
                Err(error(instance, Reason::StalePlan))
            };
        }
        {
            let state = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.acquired_view()
                || state.attempt.is_some()
                || state.retired
                || state.closed
                || state.listener.is_some()
                || state.raw.is_some()
                || state.error.is_some()
            {
                return Err(error(instance, Reason::Revoked));
            }
        }
        if shared
            .accepted
            .as_ref()
            .is_some_and(|pool| !pool.matches_original_scope(&scope.scope_identity()))
        {
            return Err(error(instance, Reason::StalePlan));
        }
        if let Some(pool) = &shared.accepted {
            if !pool.has_task_failure_receiver(instance, &scope.scope_identity()) {
                let mut error = error(instance, Reason::InvalidConfig);
                error.delivery = DeliveryState::NotSubmitted;
                return Err(error);
            }
        }
        let binding = Binding {
            instance,
            root: Arc::downgrade(root),
            scope,
        };
        shared
            .originals
            .set(binding.clone())
            .map_err(|_| error(instance, Reason::StalePlan))?;
        if let Some(pool) = &shared.accepted {
            pool.bind_original_totals(binding)?;
        }
        Ok(())
    }
}
struct Original {
    owner: Arc<Shared>,
    attempt: Arc<Attempt>,
    scope: ScopeRetirementObservation,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
    first_fault: Mutex<Option<Arc<ChannelError>>>,
    native: Mutex<
        Option<BoxFuture<'static, Result<crate::contract::lifecycle::CloseReport, ChannelError>>>,
    >,
    native_finished: AtomicBool,
    users_finished: AtomicBool,
    deadline: std::sync::OnceLock<tokio::time::Instant>,
}
mod original;

fn error(instance: ComponentInstanceId, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    error.component = Some(instance.component);
    error.delivery = DeliveryState::NotSubmitted;
    error
}

pub(super) struct Reservation {
    root: Arc<CarrierOriginalTotals>,
    original: Arc<dyn SourceCustody>,
}
impl Reservation {
    pub(super) fn new(
        root: Arc<CarrierOriginalTotals>,
        instance: ComponentInstanceId,
        original: Arc<dyn SourceCustody>,
    ) -> Result<Self, ChannelError> {
        root.reserve_source(instance, original.clone())?;
        Ok(Self { root, original })
    }
    pub(super) fn release_settled(&self) {
        self.root.release_source_if_settled(&self.original);
    }
}
