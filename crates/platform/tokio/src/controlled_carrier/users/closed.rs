//! Original user closure receipts, separate from registration credit.
use super::*;
/// Opaque original slot proof: logical view returned, the actual delivery
/// owner joined (preparation Auxiliary before publication, Business caller after),
/// actual transport driver joined (when any), and original native control closed.
#[derive(Clone)]
pub struct CarrierUsersClosed {
    slot: Arc<Slot>,
    caller: Option<TaskJoinWitness>,
}
impl CarrierUsersClosed {
    pub fn had_native_acquisition(&self) -> bool {
        self.slot
            .state()
            .users
            .as_ref()
            .is_some_and(|users| users.acquired)
    }
    pub fn matches(&self, adoption: &CarrierAdoption) -> bool {
        Arc::ptr_eq(&self.slot, &adoption.slot)
    }
    pub fn matches_caller(&self, caller: &TaskJoinWitness) -> bool {
        self.caller
            .as_ref()
            .is_some_and(|actual| actual.same_registration(caller))
    }
    /// None means no separate caller task: either no acquisition, or custody
    /// is taskless or belongs to the original transport driver. Any real exit is independently
    /// available from CarrierClosed::driver_exit().
    pub fn caller_exit(&self) -> Option<TaskJoined> {
        self.caller.as_ref().and_then(TaskJoinWitness::try_joined)
    }
}
impl CarrierAdoption {
    pub(crate) fn matches_bound_caller(&self, caller: &TaskJoinWitness) -> bool {
        self.slot
            .state()
            .users
            .as_ref()
            .and_then(|users| users.caller.as_ref())
            .is_some_and(|actual| actual.same_registration(caller))
    }
    pub fn users_closed(&self) -> Option<CarrierUsersClosed> {
        let (closed, users) = {
            let state = self.slot.state();
            (state.closed, state.users.clone())
        };
        let users = users?;
        if !closed || !users.ready() {
            return None;
        }
        Some(CarrierUsersClosed {
            slot: self.slot.clone(),
            // Before publication the real setup task owns delivery. The
            // independent Business authorization remains bound to its caller;
            // this receipt must not claim that still-live caller has joined.
            caller: users.credit_caller(),
        })
    }
}
/// Issued only by explicit successful shutdown of the one actual owner after
/// every acquired original's caller/view/driver/native facts were confirmed.
#[derive(Clone)]
pub struct CarrierOwnerUsersClosed {
    completion: Arc<super::registration::Completion>,
}
impl CarrierOwnerUsersClosed {
    pub fn matches(&self, registration: &CarrierOwnerRegistration) -> bool {
        registration.matches_users_completion(&self.completion)
    }
    pub fn first_caller_failure(&self) -> Option<TaskJoined> {
        self.completion
            .caller_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub fn had_native_acquisition(&self) -> bool {
        self.completion.acquired.load(Ordering::Acquire)
    }
}
impl CarrierOwnerRegistration {
    pub fn users_closed(&self) -> Option<CarrierOwnerUsersClosed> {
        let completion = self.users_completion();
        completion
            .users_closed
            .load(Ordering::Acquire)
            .then_some(CarrierOwnerUsersClosed { completion })
    }
}
