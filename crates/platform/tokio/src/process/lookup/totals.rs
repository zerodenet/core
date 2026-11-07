//! One actual lookup reservation in the same Host root. No executor or pool.
use super::*;
use crate::{
    contract::BoxFuture,
    controlled_carrier::{CarrierOriginalTotals, SourceCustody},
};
use std::{
    sync::{atomic::AtomicBool, Weak},
    task::Context,
};
use zero_core::component::ComponentInstanceId;

#[derive(Clone)]
struct Binding {
    instance: ComponentInstanceId,
    root: Weak<CarrierOriginalTotals>,
    scope: crate::contract::scope::TaskScopeIdentity,
}
#[derive(Default)]
pub(super) struct Custody {
    binding: OnceLock<Binding>,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
}
impl LocalProcessLookup {
    /// Cold same-Host accounting metadata, never task authority or native IO.
    /// Explicit standalone callers supply their own real Scope and root here.
    pub fn bind_original_totals(
        &self,
        instance: ComponentInstanceId,
        registrar: &WeakTaskRegistrar,
        root: &Arc<CarrierOriginalTotals>,
    ) -> Result<(), ChannelError> {
        let scope = registrar.scope_identity();
        if !self.caller.scope_identity().same_scope(&scope) {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let binding = Binding {
            instance,
            root: Arc::downgrade(root),
            scope,
        };
        if let Some(actual) = self.totals.binding.get() {
            return if actual.instance == instance
                && actual.root.ptr_eq(&binding.root)
                && actual.scope.same_scope(&binding.scope)
            {
                Ok(())
            } else {
                Err(error(Stage::Prepare, Reason::StalePlan))
            };
        }
        if self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .attempted
        {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        self.totals
            .binding
            .set(binding)
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))
    }
    pub(super) fn reserve_original_total(
        self: &Arc<Self>,
        registrar: &WeakTaskRegistrar,
    ) -> Result<(), ChannelError> {
        let binding = self.totals.binding.get().ok_or_else(|| {
            error(Stage::Prepare, Reason::Internal).with_source(std::io::Error::other(
                "actual lookup Host root was not installed",
            ))
        })?;
        if !binding.scope.same_scope(&registrar.scope_identity()) {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let root = binding
            .root
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        // Reconcile only already-proven native/join/ACK settlement. This does
        // not poll IO or close a live sibling to make room for a new lookup.
        root.reconcile();
        let original: Arc<dyn SourceCustody> = self.clone();
        root.reserve_source(binding.instance, original)
    }
    pub(super) fn release_original_total(self: &Arc<Self>) {
        if let Some(root) = self
            .totals
            .binding
            .get()
            .and_then(|binding| binding.root.upgrade())
        {
            let original: Arc<dyn SourceCustody> = self.clone();
            root.release_source_if_settled(&original);
        }
    }
}
impl SourceCustody for LocalProcessLookup {
    fn released(&self) -> bool {
        self.retired()
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        if self.retired() {
            return false;
        }
        let Some(_polling) = crate::original_poll::OriginalPoll::enter(
            &self.totals.polling,
            &self.totals.repoll,
            cx.waker(),
        ) else {
            return false;
        };
        // The body result, start gate and cancellation can never authorize
        // closing descriptors still used by the original blocking worker.
        if self.witness.get().is_some_and(TaskJoinWitness::is_joined) {
            match self.collect_native_after_join() {
                Ok(()) => {}
                Err(_) => return false, // same Native strongly retains the actual typed close failure
            }
            if self
                .witness
                .get()
                .and_then(TaskJoinWitness::try_joined)
                .is_some_and(|joined| joined.registration_released())
            {
                return self.retired();
            }
        }
        let mut waiting = self
            .totals
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| match self.witness.get().cloned() {
                Some(witness) if witness.is_joined() => Box::pin(async move {
                    witness.wait_registration_released().await;
                })
                    as BoxFuture<'static, ()>,
                Some(witness) => Box::pin(async move {
                    witness.wait().await;
                }),
                None => {
                    let changed = self.changed.clone();
                    let witness = self.witness.clone();
                    Box::pin(async move {
                        loop {
                            let notified = changed.notified();
                            tokio::pin!(notified);
                            notified.as_mut().enable();
                            if witness.get().is_some() {
                                break;
                            }
                            notified.await;
                        }
                    })
                }
            });
        if waiting.as_mut().poll(cx).is_pending() {
            *self
                .totals
                .waiting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waiting);
            false
        } else {
            // A real join wakes the next stage: native cleanup, then actual
            // registration release. No future captures this Lookup or Root.
            if !self.retired() {
                cx.waker().wake_by_ref();
            }
            true
        }
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        self.native.failure().or_else(|| {
            self.witness
                .get()
                .and_then(TaskJoinWitness::try_joined)
                .and_then(|joined| joined.result().err())
        })
    }
}
