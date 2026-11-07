//! Same Host reservation before the original software/outer acquisition starts.
use super::*;
use crate::contract::{
    scope::{ScopeRetirementObservation, WeakTaskRegistrar},
    BoxFuture,
};
use crate::controlled_carrier::{CarrierOriginalTotals, SourceCustody};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, OnceLock, Weak,
    },
    task::Context,
};
struct Binding {
    root: Weak<CarrierOriginalTotals>,
    scope: ScopeRetirementObservation,
}
#[derive(Default)]
pub(super) struct Custody {
    binding: OnceLock<Binding>,
    admission: Mutex<()>,
    charged: AtomicBool,
    unconfirmed_ancestor: AtomicBool,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
}
impl LogicalPacketProviderRegistration {
    /// One concrete software-driver custody unit, not a native FD total. The
    /// same root strongly keeps the actual original through pending retirement.
    pub fn reserve_original_totals(
        &self,
        tasks: &WeakTaskRegistrar,
        root: &Arc<CarrierOriginalTotals>,
    ) -> Result<(), ChannelError> {
        if self.is_retired() || !tasks.scope_identity().same_scope(self.device().scope()) {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        root.reconcile();
        let _admission = self.0.custody.admission.lock().unwrap();
        let weak = Arc::downgrade(root);
        if let Some(binding) = self.0.custody.binding.get() {
            if !binding.root.ptr_eq(&weak) {
                return Err(error(
                    self.instance(),
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                ));
            }
        } else {
            let scope = tasks.retirement_observation().ok_or_else(|| {
                error(
                    self.instance(),
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )
            })?;
            self.0
                .custody
                .binding
                .set(Binding { root: weak, scope })
                .map_err(|_| {
                    error(
                        self.instance(),
                        Stage::Prepare,
                        Reason::StalePlan,
                        DeliveryState::NotSubmitted,
                    )
                })?;
        }
        if !self.0.custody.charged.load(Ordering::Acquire) {
            let original: Arc<dyn SourceCustody> = self.0.clone();
            root.reserve_source(self.instance(), original)?;
            self.0.custody.charged.store(true, Ordering::Release);
        }
        Ok(())
    }
    pub fn has_original_total(&self, root: &Arc<CarrierOriginalTotals>) -> bool {
        self.0.custody.charged.load(Ordering::Acquire)
            && self
                .0
                .custody
                .binding
                .get()
                .is_some_and(|binding| binding.root.ptr_eq(&Arc::downgrade(root)))
    }
    /// Call only at the original opaque physical-ancestor acquisition boundary,
    /// after validation and before its first native operation. This monotonic
    /// observation cannot mint close/NoNative or erase the retained old debt.
    pub fn retain_unconfirmed_physical_ancestor(&self) -> Result<(), ChannelError> {
        let _admission = self.0.custody.admission.lock().unwrap();
        if !self.0.custody.charged.load(Ordering::Acquire) || self.is_retired() {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        self.0
            .custody
            .unconfirmed_ancestor
            .store(true, Ordering::Release);
        Ok(())
    }
}
impl SourceCustody for ActualLogicalPacket {
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId> {
        Some(self.device.instance())
    }
    fn released(&self) -> bool {
        // The original outer-acquisition marker and final quota reconciliation
        // share this exact gate. Scope completion cannot race a new attempt.
        let Ok(_admission) = self.custody.admission.try_lock() else {
            return false;
        };
        !self.custody.unconfirmed_ancestor.load(Ordering::Acquire)
            && self
                .custody
                .binding
                .get()
                .is_some_and(|binding| binding.scope.is_drained())
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        if self.custody.unconfirmed_ancestor.load(Ordering::Acquire) {
            return false;
        }
        let Some(_polling) = crate::original_poll::OriginalPoll::enter(
            &self.custody.polling,
            &self.custody.repoll,
            cx.waker(),
        ) else {
            return false;
        };
        let Some(binding) = self.custody.binding.get() else {
            return false;
        };
        let mut waiting = self
            .custody
            .waiting
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| {
                let scope = binding.scope.clone();
                Box::pin(async move {
                    scope.wait_drained().await;
                })
            });
        if waiting.as_mut().poll(cx).is_pending() {
            *self.custody.waiting.lock().unwrap() = Some(waiting);
            false
        } else {
            true
        }
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        self.driver
            .try_joined()
            .and_then(|joined| joined.result().err())
    }
}
