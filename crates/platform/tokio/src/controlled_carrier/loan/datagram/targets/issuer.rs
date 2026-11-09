//! Only the exact existing Host binding/private fence issues this original.
use super::*;
use crate::contract::CancellationToken;
impl CarrierOwnerBinding {
    #[expect(
        clippy::too_many_arguments,
        reason = "one exact Host original and its actual immutable Service demand"
    )]
    pub fn service_datagram_target_issuer(
        &self,
        host_fence: &Arc<CancellationToken>,
        grant: &CarrierGrant,
        actor: &TaskJoinWitness,
        binding: &UpstreamBinding,
        endpoint: &Destination,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<ServiceDatagramTargetIssuer, ChannelError> {
        let fail = || failure(grant.instance(), Reason::StalePlan);
        if !self.matches_host_cancel(host_fence)
            || !self.registration().matches_grant(grant)
            || !self
                .authority
                .permits_service_datagram_loan(binding, endpoint, call)
            || !self.scope_identity().same_scope(&actor.scope_identity())
            || !matches!(identity,RequestIdentity::Service(facts)
                if facts.origin==zero_core::request::ServiceOrigin::Runtime
                    &&facts.purpose==zero_core::request::ServicePurpose::BackgroundControl)
        {
            return Err(fail());
        }
        let issuer = ServiceDatagramTargetIssuer {
            original: Arc::downgrade(&grant.slot),
            bound: self.clone(),
            actor: actor.clone(),
            endpoint: endpoint.clone(),
            binding: binding.clone(),
            identity: identity.clone(),
            call: call.clone(),
        };
        issuer.check()?;
        Ok(issuer)
    }
}
impl ServiceDatagramTargetIssuer {
    /// Actual Host-registered receiver for metadata source lookup only. The
    /// returned identity grants no resolver, native, cached or task authority.
    pub fn instance(&self) -> zero_core::component::ComponentInstanceId {
        self.bound.registration().instance()
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.binding
    }
    pub fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    pub(super) fn check_installed(&self, grant: &CarrierGrant) -> Result<(), ChannelError> {
        let fail = || failure(grant.instance(), Reason::StalePlan);
        if !self.original.ptr_eq(&Arc::downgrade(&grant.slot))
            || !self.bound.registration().matches_grant(grant)
            || !self.bound.authority.available()
            || !self
                .bound
                .registration()
                .bound()
                .is_some_and(|current| current.matches_authority(&self.bound.authority))
            || self.actor.is_joined()
            || !self.actor.scope_identity().resource_open()
            || self.call.cancel.is_cancelled()
            || self
                .call
                .deadline
                .is_some_and(|end| tokio::time::Instant::now() >= end)
        {
            return Err(fail());
        }
        let state = grant.slot.state();
        if grant.slot.cancel.is_cancelled()
            || !state.service_target_selected
            || state.closed
            || state.opening
            || state.spawning
            || state.spawn_attempted
            || state.protocol.is_none()
            || state.loan.is_none()
            || state.binding.as_ref() != Some(&self.binding)
            || state.endpoint.as_ref() != Some(&self.endpoint)
            || state.control.is_some()
            || state.packet.is_some()
            || state.unknown.is_some()
            || !state
                .auxiliary
                .as_ref()
                .is_some_and(|actual| actual.same_registration(&self.actor))
            || !state
                .users
                .as_ref()
                .is_some_and(|users| users.live() && users.transferred() && users.acquired())
        {
            return Err(fail());
        }
        Ok(())
    }
    pub(super) fn check(&self) -> Result<Arc<Slot>, ChannelError> {
        self.check_phase(false)
    }
    pub(super) fn check_selected(&self) -> Result<Arc<Slot>, ChannelError> {
        self.check_phase(true)
    }
    pub(super) fn select_once(&self) -> Result<(), ChannelError> {
        let slot = self.check()?;
        let mut state = slot.state();
        if state.service_target_selected {
            return Err(failure(slot.instance, Reason::StalePlan));
        }
        state.service_target_selected = true;
        Ok(())
    }
    fn check_phase(&self, selected: bool) -> Result<Arc<Slot>, ChannelError> {
        let instance = self.bound.registration().instance();
        let fail = || failure(instance, Reason::StalePlan);
        let slot = self.original.upgrade().ok_or_else(fail)?;
        if !self.bound.authority.available()
            || slot.cancel.is_cancelled()
            || self.call.cancel.is_cancelled()
            || self.actor.is_joined()
            || !self.actor.scope_identity().resource_open()
            || self
                .call
                .deadline
                .is_some_and(|end| tokio::time::Instant::now() >= end)
            || !self
                .bound
                .registration()
                .bound()
                .is_some_and(|current| current.matches_authority(&self.bound.authority))
        {
            return Err(fail());
        }
        let state = slot.state();
        if state.service_target_selected != selected
            || state.closed
            || state.opening
            || state.spawning
            || state.spawn_attempted
            || state.loan.is_some()
            || state.control.is_some()
            || state.packet.is_some()
            || state.unknown.is_some()
            || state.protocol.is_none()
            || !state
                .auxiliary
                .as_ref()
                .is_some_and(|actual| actual.same_registration(&self.actor))
            || !state
                .users
                .as_ref()
                .is_some_and(|users| users.live() && users.transferred() && !users.acquired())
        {
            return Err(fail());
        }
        drop(state);
        Ok(slot)
    }
}
