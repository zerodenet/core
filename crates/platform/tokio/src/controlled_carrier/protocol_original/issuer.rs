use super::*;
use crate::contract::{
    network::{CarrierContext, UpstreamTransport},
    scope::TaskRegistrar,
};
impl CarrierGrant {
    /// This concrete borrowed-datagram phase is installed in the same Slot
    /// before its genuine setup Aux and lower-Loan acquisition may begin.
    pub fn protocol_original(
        &self,
        upstream: &UpstreamTransport,
        observer: PendingCarrierObserver,
        endpoint: &zero_core::request::Destination,
        call: &CarrierContext,
        tasks: &TaskRegistrar,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<ProtocolOriginal, ChannelError> {
        let failure = || {
            error(
                self.slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
        };
        let owner = self.slot.owner.upgrade().ok_or_else(failure)?;
        let bound = { owner.state().binding.clone() }.ok_or_else(failure)?;
        let actual = { self.slot.state().protocol.clone() }.ok_or_else(failure)?;
        let scope = bound.protocol_scope_identity();
        let actual_observer = upstream.protocol_original_observer();
        if !actual_observer
            .as_ref()
            .is_some_and(|actual| observer.same_observer(actual))
            || !self.slot.root_bound
            || !retirement.matches_phase(&actual)
            || !scope.matches(tasks)
            || !bound.permits_service_datagram_loan(upstream.binding(), endpoint, call)
            || call.cancel.is_cancelled()
            || owner.cancel.is_cancelled()
            || self.slot.acquisition_sealed()
        {
            return Err(failure());
        }
        Ok(ProtocolOriginal::carrier_original(
            self.adoption(),
            scope,
            upstream.original_binding(),
            observer,
            retirement.clone(),
        ))
    }
}
