//! One-way delivery of the original Business view after its real setup task.
//! Native authorization remains bound to the admitted Business caller.
mod closed;
mod job;
mod publication;
mod source;
mod spawn;
use super::{error, slot::Slot, CarrierGrant, CarrierUserView, CarrierUsersClosed};
use crate::contract::{scope::TaskJoinWitness, ChannelError, DeliveryState, Reason, Stage};
pub use closed::CarrierPreparationClosed;
pub(in crate::controlled_carrier) use job::exit::{accept_job_exit, try_accept_job_exit};
pub(in crate::controlled_carrier) use job::staged::cancel_staged;
pub use job::{CarrierPreparationJob, CarrierPreparationJobRetained, StagedCarrierPreparationJob};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock, Weak,
};

pub(super) struct Preparation {
    pub(super) expected_caller: Option<TaskJoinWitness>,
    pub(super) auxiliary: OnceLock<TaskJoinWitness>,
    pub(super) published: AtomicBool,
    pub(super) job: OnceLock<Arc<job::Job>>,
    pending: OnceLock<crate::contract::network::PendingCarrierObserver>,
}
/// Sealed delivery state for exactly one original view and setup registration.
/// It cannot authorize native I/O, replace the Business caller, or close a task.
#[derive(Clone)]
pub struct CarrierPreparation {
    slot: Weak<Slot>,
    state: Arc<Preparation>,
}
impl CarrierGrant {
    pub fn preparation(&self) -> Option<CarrierPreparation> {
        let state = self.slot.state();
        Some(CarrierPreparation {
            slot: Arc::downgrade(&self.slot),
            state: state.users.as_ref()?.preparation.clone()?,
        })
    }
    /// Same transferred Service custody with a real pre-delivery Auxiliary.
    pub fn claim_preparing_transferred_view(
        &mut self,
    ) -> Result<(CarrierUserView, CarrierPreparation), ChannelError> {
        let allowed = self.slot.owner.upgrade().is_some_and(|owner| {
            owner
                .state()
                .binding
                .as_ref()
                .is_some_and(|binding| binding.preparation_available())
        });
        if !allowed {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let view = self.claim_transferred_view()?;
        let state = Arc::new(Preparation {
            expected_caller: None,
            auxiliary: OnceLock::new(),
            published: AtomicBool::new(false),
            job: OnceLock::new(),
            pending: OnceLock::new(),
        });
        self.slot
            .state()
            .users
            .as_mut()
            .expect("unique Service view installed")
            .preparation = Some(state.clone());
        Ok((
            view,
            CarrierPreparation {
                slot: Arc::downgrade(&self.slot),
                state,
            },
        ))
    }
    /// Claim before spawning this original's setup Auxiliary or performing I/O.
    /// The supplied caller is checked again by the actual Business ticket;
    /// claiming this view never binds a ticket or admits a session.
    pub fn claim_preparing_user_view(
        &mut self,
        caller: &TaskJoinWitness,
    ) -> Result<(CarrierUserView, CarrierPreparation), ChannelError> {
        let allowed = self.slot.owner.upgrade().is_some_and(|owner| {
            owner
                .state()
                .binding
                .as_ref()
                .is_some_and(|binding| binding.preparation_available())
        });
        if !allowed || caller.is_joined() {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let view = self.claim_user_view()?;
        let state = Arc::new(Preparation {
            expected_caller: Some(caller.clone()),
            auxiliary: OnceLock::new(),
            published: AtomicBool::new(false),
            job: OnceLock::new(),
            pending: OnceLock::new(),
        });
        self.slot
            .state()
            .users
            .as_mut()
            .expect("unique view installed")
            .preparation = Some(state.clone());
        Ok((
            view,
            CarrierPreparation {
                slot: Arc::downgrade(&self.slot),
                state,
            },
        ))
    }
}
impl CarrierPreparation {
    /// A weak reference to this preparation's existing finite caller ledger.
    /// No claim is released and no I/O permission is issued by this metadata.
    pub fn bind_pending_observer(
        &self,
        observer: crate::contract::network::PendingCarrierObserver,
    ) -> Result<(), ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(|| {
            ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                crate::contract::Impact::Component,
            )
        })?;
        let fail = || {
            error(
                slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            )
        };
        if self.is_published()
            || slot.cancel.is_cancelled()
            || self
                .state
                .auxiliary
                .get()
                .is_none_or(TaskJoinWitness::is_joined)
        {
            return Err(fail());
        }
        self.state.pending.set(observer).map_err(|_| fail())
    }
    pub fn pending_observer(
        &self,
        caller: &TaskJoinWitness,
    ) -> Result<Option<crate::contract::network::PendingCarrierObserver>, ChannelError> {
        self.preparing_caller(caller)?;
        Ok(self.state.pending.get().cloned())
    }
    /// Observe only the real setup task already installed on this exact grant.
    /// The original ticket must first have bound the same admitted caller.
    pub fn preparing_caller(
        &self,
        caller: &TaskJoinWitness,
    ) -> Result<TaskJoinWitness, ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(|| {
            ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                crate::contract::Impact::Component,
            )
        })?;
        let fail = || {
            error(
                slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            )
        };
        if self.is_published()
            || !self
                .state
                .expected_caller
                .as_ref()
                .is_some_and(|actual| actual.same_registration(caller))
            || caller.is_joined()
            || slot.cancel.is_cancelled()
        {
            return Err(fail());
        }
        let auxiliary = self.state.auxiliary.get().ok_or_else(fail)?;
        if auxiliary.is_joined() || !auxiliary.scope_identity().resource_open() {
            return Err(fail());
        }
        let state = slot.state();
        if state.closed
            || !state.users.as_ref().is_some_and(|users| {
                users.caller_matches(caller)
                    && users
                        .preparation
                        .as_ref()
                        .is_some_and(|p| Arc::ptr_eq(p, &self.state))
            })
            || !state
                .auxiliary
                .as_ref()
                .is_some_and(|actual| actual.same_registration(auxiliary))
        {
            return Err(fail());
        }
        Ok(auxiliary.clone())
    }
    pub fn is_published(&self) -> bool {
        self.state.published.load(Ordering::Acquire)
    }
    /// Positive closure of the same unpublished preparation. This does not
    /// claim that its still-live Business caller has joined or received a view.
    pub fn matches_unpublished_closed(&self, closed: &CarrierUsersClosed) -> bool {
        if self.is_published() {
            return false;
        }
        let Some(slot) = self.slot.upgrade() else {
            return false;
        };
        if !closed.matches(&super::CarrierAdoption { slot: slot.clone() }) {
            return false;
        }
        match self.state.auxiliary.get() {
            Some(actual) => {
                closed.matches_caller(actual)
                    && actual.try_joined().is_some_and(|joined| {
                        actual.matches_joined(&joined) && joined.registration_released()
                    })
            }
            None => {
                let state = slot.state();
                state.users.as_ref().is_some_and(|users| !users.acquired())
                    && state.closed
                    && !state.spawning
                    && state.auxiliary.is_none()
                    && state
                        .users
                        .as_ref()
                        .and_then(|users| users.preparation.as_ref())
                        .is_some_and(|preparation| Arc::ptr_eq(preparation, &self.state))
            }
        }
    }
}
