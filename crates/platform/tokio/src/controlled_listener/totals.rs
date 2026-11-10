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
/// Weak authority to settle this exact provider after its own native/users/ACK
/// outcome. It cannot keep the provider, listener or global book alive.
#[derive(Clone)]
pub(super) struct SettledSourceReturn {
    root: Weak<CarrierOriginalTotals>,
    original: Weak<dyn SourceCustody>,
}
impl SettledSourceReturn {
    pub(super) fn release_settled(&self) {
        if let (Some(root), Some(original)) = (self.root.upgrade(), self.original.upgrade()) {
            root.release_source_if_settled(&original);
        }
    }
}
impl Reservation {
    pub(super) fn returner(&self) -> SettledSourceReturn {
        SettledSourceReturn {
            root: Arc::downgrade(&self.root),
            original: Arc::downgrade(&self.original),
        }
    }

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

/// Strong custody of the SAME existing Source original, with a weak Root.
/// Holding Root here would create Root -> parent protocol ledger -> token ->
/// Root for managed callers. An expired weak Root is never a returned fact.
#[derive(Clone)]
pub(crate) struct RegisteredSourceOriginal {
    root: Weak<CarrierOriginalTotals>,
    original: Arc<dyn SourceCustody>,
}
impl SettledSourceReturn {
    pub(crate) fn observe_registered(&self) -> Result<RegisteredSourceOriginal, ChannelError> {
        let root = self
            .root
            .upgrade()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?;
        let original = self
            .original
            .upgrade()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?;
        if !root.has_source_original(&original) && !original.was_root_returned() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        Ok(RegisteredSourceOriginal {
            root: Arc::downgrade(&root),
            original,
        })
    }
}
impl RegisteredSourceOriginal {
    pub(crate) fn same_root(&self, root: &Arc<CarrierOriginalTotals>) -> bool {
        self.root.ptr_eq(&Arc::downgrade(root))
    }
    pub(crate) fn same_original(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.root, &other.root) && Arc::ptr_eq(&self.original, &other.original)
    }
    pub(crate) fn is_settled(&self) -> bool {
        self.original.was_root_returned()
    }
    pub(crate) async fn wait_settled(&self) -> Result<(), ChannelError> {
        let notify = self.original.root_return_notifications().ok_or_else(|| {
            ChannelError::new(Stage::Close, Reason::Unsupported, Impact::Component)
        })?;
        loop {
            let changed = notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_settled() {
                return Ok(());
            }
            // Upgrade/drop before awaiting; no actual operation future or
            // notification owns a strong Root backedge across Pending.
            if self.root.upgrade().is_none() {
                return Err(ChannelError::new(
                    Stage::Close,
                    Reason::Revoked,
                    Impact::Component,
                ));
            }
            changed.await;
        }
    }
}
