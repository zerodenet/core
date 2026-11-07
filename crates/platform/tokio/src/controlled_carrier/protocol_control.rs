//! Exact noncached Service-datagram original setup, before its first I/O.
use super::*;
use crate::contract::scope::{TaskJoinWitness, TaskRegistrar};
impl CarrierGrant {
    pub fn install_datagram_protocol_control(
        &self,
        tasks: &TaskRegistrar,
        actor: &TaskJoinWitness,
        control: crate::contract::lifecycle::BoxChannelControl,
    ) -> Result<CarrierProtocolRetirement, RejectedOriginalProtocolControl> {
        let valid_bound = self
            .slot
            .owner
            .upgrade()
            .and_then(|owner| owner.state().binding.clone())
            .is_some_and(|bound| bound.permits_datagram_protocol_control(tasks));
        let mut state = self.slot.state();
        if !valid_bound
            || self.slot.cancel.is_cancelled()
            || actor.is_joined()
            || !actor.scope_identity().same_scope(&tasks.scope_identity())
            || !state
                .auxiliary
                .as_ref()
                .is_some_and(|actual| actual.same_registration(actor))
            || state.protocol.is_some()
            || state.opening
            || state.closed
            || state.spawn_attempted
            || state.loan.is_some()
            || state.packet.is_some()
            || state.control.is_some()
            || state.unknown.is_some()
        {
            drop(state);
            return Err(RejectedOriginalProtocolControl {
                control,
                error: error(
                    self.slot.instance,
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                ),
            });
        }
        let phase = crate::original_protocol::Phase::new(control, self.slot.instance);
        let handle = phase.handle();
        let installed = self.slot.protocol_stop.set(phase.cancellation());
        assert!(installed.is_ok(), "one protocol phase per exact original");
        state.protocol = Some(phase);
        Ok(handle)
    }
}

impl CarrierGrants {
    /// Store the unique inert protocol controller in this same actual Slot and
    /// Root before registering its actor or allowing any first IO. Only the
    /// existing concrete per-connection Service Datagram declaration qualifies.
    pub fn reserve_protocol_original(
        &self,
        tasks: &TaskRegistrar,
        control: crate::contract::lifecycle::BoxChannelControl,
    ) -> Result<(CarrierGrant, CarrierProtocolRetirement), RejectedOriginalProtocolControl> {
        let bound = { self.0.state().binding.clone() };
        if !bound
            .as_ref()
            .is_some_and(|bound| bound.permits_datagram_protocol_control(tasks))
        {
            return Err(RejectedOriginalProtocolControl {
                control,
                error: error(
                    self.0.instance,
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                ),
            });
        }
        match self.reserve_with_protocol(Some(control)) {
            Ok(grant) => {
                let retirement = grant
                    .slot
                    .state()
                    .protocol
                    .as_ref()
                    .expect("paid original phase installed before publication")
                    .handle();
                Ok((grant, retirement))
            }
            Err((error, returned)) => Err(RejectedOriginalProtocolControl {
                error,
                control: returned.expect("refused original did not consume control"),
            }),
        }
    }
}
