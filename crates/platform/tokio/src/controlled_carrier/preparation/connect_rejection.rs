//! A received candidate result is distinct from an unhandled initializer fault.
use super::*;
impl CarrierPreparation {
    /// The actual result receiver already owns this exact original native
    /// connect error. Keep its real failed TaskJoined; only its later duplicate
    /// component-fatal projection is suppressed after full original settlement.
    /// This cannot ACK a Scope, close native IO, publish a view or refund N.
    pub fn receive_connect_rejection(
        &self,
        source: &Arc<ChannelError>,
    ) -> Result<(), ChannelError> {
        self.retain_connect_rejection(source, true)
    }
    pub(in crate::controlled_carrier) fn retain_connect_rejection_before_ack(
        &self,
        source: &Arc<ChannelError>,
    ) -> Result<(), ChannelError> {
        self.retain_connect_rejection(source, false)
    }
    fn retain_connect_rejection(
        &self,
        source: &Arc<ChannelError>,
        settled: bool,
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
        if crate::controlled_carrier::original_connect_error(source).is_none()
            || self.is_published()
        {
            return Err(fail());
        }
        let auxiliary = self.state.auxiliary.get().ok_or_else(fail)?;
        let joined = auxiliary.try_joined().ok_or_else(fail)?;
        let original = joined.result().err().ok_or_else(fail)?;
        if !auxiliary.matches_joined(&joined)
            || (settled && (!joined.registration_released() || !joined.failure_accepted()))
            || !original
                .retained_source()
                .is_some_and(|actual| Arc::ptr_eq(actual, source))
        {
            return Err(fail());
        }
        let state = slot.state();
        if state.closed
            || (settled && !state.auxiliary_accepted)
            || !state
                .auxiliary
                .as_ref()
                .is_some_and(|actual| actual.same_registration(auxiliary))
            || !state.users.as_ref().is_some_and(|users| {
                users
                    .preparation
                    .as_ref()
                    .is_some_and(|actual| Arc::ptr_eq(actual, &self.state))
            })
            || !state
                .auxiliary_exit
                .as_ref()
                .is_some_and(|actual| auxiliary.matches_joined(actual))
        {
            return Err(fail());
        }
        drop(state);
        let repeated = self.state.connect_rejection.set(joined).err();
        if repeated.as_ref().is_some_and(|actual| {
            self.state
                .connect_rejection
                .get()
                .is_none_or(|received| received.id() != actual.id())
        }) {
            return Err(fail());
        }
        drop(repeated);
        Ok(())
    }
}
