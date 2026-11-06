//! Same original delivery, only after real preparation join and Host ACK.
use super::*;
impl CarrierPreparation {
    /// Only a successfully completed and actually ACKed original Auxiliary can
    /// deliver the sole view. The caller must still be the admitted live caller.
    /// There is no inverse operation and dropping a token changes no facts.
    pub fn publish(&self, caller: &TaskJoinWitness) -> Result<(), ChannelError> {
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
        if !self
            .state
            .expected_caller
            .as_ref()
            .is_some_and(|actual| actual.same_registration(caller))
            || caller.is_joined()
            || slot.cancel.is_cancelled()
        {
            return Err(fail());
        }
        let actual = self.state.auxiliary.get().ok_or_else(fail)?;
        let joined = actual.try_joined().ok_or_else(fail)?;
        if !actual.matches_joined(&joined)
            || joined.result().is_err()
            || !joined.registration_released()
        {
            return Err(fail());
        }
        let state = slot.state();
        let users = state.users.as_ref().ok_or_else(fail)?;
        if state.closed
            || !state.auxiliary_accepted
            || !state
                .auxiliary
                .as_ref()
                .is_some_and(|w| w.same_registration(actual))
            || !users.live()
            || !users.acquired()
            || !users.caller_matches(caller)
            || !users
                .preparation
                .as_ref()
                .is_some_and(|p| Arc::ptr_eq(p, &self.state))
            || self.state.published.swap(true, Ordering::AcqRel)
        {
            return Err(fail());
        }
        drop(state);
        slot.notify();
        Ok(())
    }
    /// Service keeps its existing transferred controller after actual setup ACK.
    pub fn publish_transferred(&self) -> Result<(), ChannelError> {
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
        if self.state.expected_caller.is_some() || slot.cancel.is_cancelled() {
            return Err(fail());
        }
        let actual = self.state.auxiliary.get().ok_or_else(fail)?;
        let joined = actual.try_joined().ok_or_else(fail)?;
        if !actual.matches_joined(&joined)
            || joined.result().is_err()
            || !joined.registration_released()
        {
            return Err(fail());
        }
        let state = slot.state();
        let users = state.users.as_ref().ok_or_else(fail)?;
        if state.closed
            || !state.auxiliary_accepted
            || !state
                .auxiliary
                .as_ref()
                .is_some_and(|w| w.same_registration(actual))
            || !users.live()
            || !users.acquired()
            || !users.transferred()
            || !users
                .preparation
                .as_ref()
                .is_some_and(|p| Arc::ptr_eq(p, &self.state))
            || self.state.published.swap(true, Ordering::AcqRel)
        {
            return Err(fail());
        }
        drop(state);
        slot.notify();
        Ok(())
    }
}
