//! Same original prepared IO really closed before being delivered to its caller.
use super::*;
use crate::controlled_carrier::{CarrierAdoption, CarrierClosed};
/// This is not a receipt of the Business caller's exit. That actual task and
/// its registration ACK remain independent, and published views cannot get it.
#[derive(Clone)]
pub struct CarrierPreparationClosed {
    preparation: CarrierPreparation,
    caller: TaskJoinWitness,
}
impl CarrierAdoption {
    pub fn unpublished_preparation_closed_for(
        &self,
        caller: &TaskJoinWitness,
    ) -> Option<CarrierPreparationClosed> {
        let preparation = {
            let state = self.slot.state();
            let users = state.users.as_ref()?;
            if !users.caller_matches(caller) {
                return None;
            }
            CarrierPreparation {
                slot: Arc::downgrade(&self.slot),
                state: users.preparation.clone()?,
            }
        };
        let proof = CarrierPreparationClosed {
            preparation,
            caller: caller.clone(),
        };
        let native = self.closed()?;
        let users = self.users_closed()?;
        proof
            .matches_receipts(&native, &users, caller)
            .then_some(proof)
    }
}
impl CarrierPreparationClosed {
    /// Recheck exact original native/users receipts, original expected caller,
    /// never-published phase and actual preparation's join/Host ACK.
    pub fn matches_receipts(
        &self,
        native: &CarrierClosed,
        users: &CarrierUsersClosed,
        caller: &TaskJoinWitness,
    ) -> bool {
        let Some(slot) = self.preparation.slot.upgrade() else {
            return false;
        };
        if !self.caller.same_registration(caller)
            || !self
                .preparation
                .state
                .expected_caller
                .as_ref()
                .is_some_and(|actual| actual.same_registration(caller))
            || !self.preparation.matches_unpublished_closed(users)
        {
            return false;
        }
        let adoption = CarrierAdoption { slot };
        native.matches(&adoption) && users.matches(&adoption)
    }
}
