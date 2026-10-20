//! A read-only lookup, never a replacement for Slot start/current admission.
use super::*;
use zero_platform_tokio::{
    contract::{component::ControlledStreamDemandClass, network::UpstreamBinding},
    controlled_carrier::CarrierGrant,
};
impl HostResources {
    /// The caller must first verify actual Slot start success/current membership
    /// and its sealed business demand. This method proves only the published
    /// canonical resource binding to this exact original physical reservation.
    pub(in crate::component) fn registered_carrier(
        &self,
        instance: ComponentInstanceId,
        endpoint: &Destination,
        binding: &UpstreamBinding,
        demand: ControlledStreamDemandClass,
        grant: &CarrierGrant,
    ) -> Result<CarrierOwnerBinding, ChannelError> {
        let fail = || {
            error(
                self.component,
                Reason::StalePlan,
                "no exact registered carrier owner for this physical grant",
            )
        };
        let (actual, scope) = self.carriers.identity.as_ref().ok_or_else(fail)?;
        if instance != *actual || grant.instance() != *actual || !scope.registration_open() {
            return Err(fail());
        }
        let candidates: Vec<_> = {
            let records = self
                .carriers
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            records
                .iter()
                .filter_map(|(ordinal, entry)| {
                    let ResourceDeclaration::ControlledOutboundStream {
                        endpoint: expected_endpoint,
                        binding: expected_binding,
                        demand: expected_demand,
                        ..
                    } = self.carriers.declarations.get(ordinal)?
                    else {
                        return None;
                    };
                    if endpoint != expected_endpoint
                        || binding != expected_binding
                        || demand != *expected_demand
                        || !entry.actual.matches_grant(grant)
                    {
                        return None;
                    }
                    entry
                        .bound
                        .clone()
                        .map(|bound| (entry.actual.clone(), bound))
                })
                .collect()
        };
        let [(registration, bound)] = candidates.as_slice() else {
            return Err(fail());
        };
        // Both getters may finally release original owners or flush I/O wakes.
        // No resource/cleanup/monitor lock encloses either one.
        if !registration
            .observe()
            .is_some_and(|owner| owner.is_live() && !owner.is_stopped())
            || !registration
                .bound()
                .is_some_and(|current| current.matches_registration(registration))
            || !bound.matches_registration(registration)
            || !bound.matches_host_cancel(self.carriers.host_cancel.as_ref().ok_or_else(fail)?)
            || bound.host_cancelled()
        {
            return Err(fail());
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || self.cancel.is_cancelled() || state.start_deadline.is_some() {
            return Err(error(
                self.component,
                Reason::Revoked,
                "carrier binding is not available outside its completed grant phase",
            ));
        }
        Ok(bound.clone())
    }
}
