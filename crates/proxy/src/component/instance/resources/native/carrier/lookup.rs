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
        if instance != *actual || grant.instance() != *actual || !scope.resource_open() {
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
                    if !entry.actual.matches_grant(grant) {
                        return None;
                    }
                    entry
                        .bound
                        .clone()
                        .map(|bound| (entry.actual.clone(), bound, *ordinal))
                })
                .collect()
        };
        let [(registration, bound, ordinal)] = candidates.as_slice() else {
            return Err(fail());
        };
        let declaration = self.carriers.declarations.get(ordinal).ok_or_else(fail)?;
        if !bound.matches_declaration(declaration) || bound.demand() != demand {
            return Err(fail());
        }
        match bound.fixed_endpoint_binding() {
            Some((expected_endpoint, expected_binding))
                if expected_endpoint == endpoint && expected_binding == binding => {}
            None if demand == ControlledStreamDemandClass::Business
                && bound.matches_direct(endpoint, binding) => {}
            _ => return Err(fail()),
        }
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

impl HostResources {
    /// Private host-side minting only. ResourceAccess never returns this issuer;
    /// actual Slot/Engine start, membership and demand validation remain required.
    pub(in crate::component) fn registered_business_issuer(
        &self,
        instance: ComponentInstanceId,
        endpoint: &Destination,
        binding: &UpstreamBinding,
        grant: &CarrierGrant,
    ) -> Result<
        (
            CarrierOwnerBinding,
            zero_platform_tokio::controlled_carrier::CarrierBusinessIssuer,
        ),
        ChannelError,
    > {
        let bound = self.registered_carrier(
            instance,
            endpoint,
            binding,
            ControlledStreamDemandClass::Business,
            grant,
        )?;
        let host_cancel = self
            .carriers
            .host_cancel
            .as_ref()
            .ok_or_else(|| self.denied())?;
        // The original Arc is never exposed through the protocol resource trait
        // or bound descriptor. A caller-created cancellation fence cannot match.
        let issuer = if bound.fixed_endpoint_binding().is_some() {
            bound.business_issuer(host_cancel, grant)?
        } else {
            bound.direct_business_issuer(host_cancel, grant, endpoint, binding)?
        };
        Ok((bound, issuer))
    }
}
