//! Bind only the exact declared Service and its actual original preparation.
use super::*;
impl CarrierServiceGrant {
    pub(in crate::controlled_carrier) fn bind_caller(
        &self,
        slot: &Arc<Slot>,
    ) -> Result<(), ChannelError> {
        if !Weak::ptr_eq(&self.slot, &Arc::downgrade(slot)) {
            return Err(failure(slot.instance, Reason::StalePlan));
        }
        let identity = Arc::downgrade(&self.demand);
        let cancel = self.demand.cancel.clone();
        let mut state = slot.state();
        if state.closed || slot.cancel.is_cancelled() || state.business_identity.is_some() {
            return Err(failure(slot.instance, Reason::Revoked));
        }
        if state
            .service_identity
            .as_ref()
            .is_some_and(|actual| !Weak::ptr_eq(actual, &identity))
        {
            return Err(failure(slot.instance, Reason::StalePlan));
        }
        let prepared_direct = self.direct
            && self
                .binding
                .matches_direct_service(&self.endpoint, &self.upstream)
            && self.binding.registration().matches_adoption(
                &crate::controlled_carrier::CarrierAdoption { slot: slot.clone() },
            )
            && self
                .binding
                .scope_identity()
                .same_scope(&self.demand.caller.scope_identity())
            && !self.demand.caller.is_joined()
            && state
                .auxiliary
                .as_ref()
                .is_some_and(|actual| actual.same_registration(&self.demand.caller))
            && state
                .users
                .as_ref()
                .is_some_and(|users| users.preparing_transferred_for(&self.demand.caller));
        if !prepared_direct
            && !state
                .users
                .as_mut()
                .is_some_and(|users| users.bind(self.demand.caller.clone()))
        {
            return Err(failure(slot.instance, Reason::InvalidConfig));
        }
        if state.service_identity.is_none() {
            state.service_identity = Some(identity);
            state.business_cancel = Some(cancel);
        }
        Ok(())
    }
}
