//! Actual host-fence capability for one original reservation, never ID-only authority.
use super::{error, slot::Slot, CarrierGrant, CarrierOwnerBinding};
use crate::contract::{
    component::ControlledStreamDemandClass,
    network::{CarrierContext, UpstreamBinding},
    CancellationToken, ChannelError, DeliveryState, Reason, Stage,
};
use std::sync::{Arc, Weak};
use tokio::time::Instant;
use zero_core::{component::SessionId, request::Destination};

/// A host-private Engine/Slot validator. Implementing this trait alone grants
/// nothing: issuing requires the original registered host fence and slot issuer.
pub trait CarrierBusinessDemand: Send + Sync {
    fn check(&self) -> Result<(), ChannelError>;
    fn session(&self) -> SessionId;
    fn cancellation(&self) -> CancellationToken;
    fn deadline(&self) -> Option<Instant>;
}
/// Keep inside the actual host. ResourceAccess must not expose this to protocols.
/// It is tied to the registered fence allocation and this one physical slot.
pub struct CarrierBusinessIssuer {
    binding: CarrierOwnerBinding,
    slot: Weak<Slot>,
    endpoint: Destination,
    upstream: UpstreamBinding,
}
#[derive(Clone)]
pub struct CarrierBusinessGrant {
    binding: CarrierOwnerBinding,
    slot: Weak<Slot>,
    demand: Arc<dyn CarrierBusinessDemand>,
    endpoint: Destination,
    upstream: UpstreamBinding,
    caller: Option<crate::contract::scope::TaskJoinWitness>,
}
impl CarrierOwnerBinding {
    /// Physical capability construction requires the exact private HostResources
    /// fence allocation, not a cloned token in a new Arc or a public SessionId.
    /// The production host must additionally verify its actual Engine receipt,
    /// started/current Slot and request facts before issuing a business grant.
    pub fn business_issuer(
        &self,
        host_fence: &Arc<CancellationToken>,
        grant: &CarrierGrant,
    ) -> Result<CarrierBusinessIssuer, ChannelError> {
        let (endpoint, binding) = self.fixed_endpoint_binding().ok_or_else(|| {
            error(
                grant.instance(),
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            )
        })?;
        self.issuer_for(host_fence, grant, endpoint.clone(), binding.clone())
    }
    /// The real host must derive this literal candidate from its original
    /// business Direct resolution, never from a caller's claimed SessionId.
    pub fn direct_business_issuer(
        &self,
        host_fence: &Arc<CancellationToken>,
        grant: &CarrierGrant,
        endpoint: &Destination,
        binding: &UpstreamBinding,
    ) -> Result<CarrierBusinessIssuer, ChannelError> {
        if !self.matches_direct(endpoint, binding) {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        self.issuer_for(host_fence, grant, endpoint.clone(), binding.clone())
    }
    fn issuer_for(
        &self,
        host_fence: &Arc<CancellationToken>,
        grant: &CarrierGrant,
        endpoint: Destination,
        upstream: UpstreamBinding,
    ) -> Result<CarrierBusinessIssuer, ChannelError> {
        if self.demand() != ControlledStreamDemandClass::Business
            || !self.matches_host_cancel(host_fence)
            || self.host_cancelled()
            || !self.registration().matches_grant(grant)
            || !self.scope_identity().resource_open()
        {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        grant.check()?;
        Ok(CarrierBusinessIssuer {
            binding: self.clone(),
            slot: Arc::downgrade(&grant.slot),
            endpoint,
            upstream,
        })
    }
}
impl CarrierBusinessIssuer {
    /// Possession of the actual host issuer is essential; a caller's successful
    /// validator, synthetic ID or self-bound foreign owner cannot replace it.
    pub fn authorize(
        self,
        demand: Arc<dyn CarrierBusinessDemand>,
    ) -> Result<CarrierBusinessGrant, ChannelError> {
        self.authorize_inner(demand, None)
    }
    pub fn authorize_with_caller(
        self,
        demand: Arc<dyn CarrierBusinessDemand>,
        caller: crate::contract::scope::TaskJoinWitness,
    ) -> Result<CarrierBusinessGrant, ChannelError> {
        self.authorize_inner(demand, Some(caller))
    }
    fn authorize_inner(
        self,
        demand: Arc<dyn CarrierBusinessDemand>,
        caller: Option<crate::contract::scope::TaskJoinWitness>,
    ) -> Result<CarrierBusinessGrant, ChannelError> {
        let grant = CarrierBusinessGrant {
            binding: self.binding,
            slot: self.slot,
            demand,
            endpoint: self.endpoint,
            upstream: self.upstream,
            caller,
        };
        grant.check_liveness()?;
        let slot = grant.slot.upgrade().ok_or_else(|| {
            error(
                grant.binding.registration().instance(),
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            )
        })?;
        // Install the actual caller beside the already claimed logical view
        // before returning the host ticket. Native prepare rechecks this same
        // registration; no caller observation can precede this durable binding.
        grant.bind_caller(&slot)?;
        Ok(grant)
    }
}
impl CarrierBusinessGrant {
    /// Weak evidence of this exact issued ticket's original owner. This cannot
    /// construct another ticket, native control, task or completion receipt.
    pub fn owner_registration(&self) -> super::CarrierOwnerRegistration {
        self.binding.registration().clone()
    }
    pub(super) fn bind_caller(&self, slot: &Arc<Slot>) -> Result<(), ChannelError> {
        // Trait callbacks can be arbitrary library code. Sample them before
        // taking the original state lock and retain only the original token and
        // weak allocation identity, never the demand/Engine/Slot itself.
        let cancel = self.cancellation();
        let identity = Arc::downgrade(&self.demand);
        let mut state = slot.state();
        if state
            .business_identity
            .as_ref()
            .is_some_and(|actual| !Weak::ptr_eq(actual, &identity))
        {
            return Err(error(
                slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if let Some(caller) = &self.caller {
            if !state
                .users
                .as_mut()
                .is_some_and(|users| users.bind(caller.clone()))
            {
                return Err(error(
                    slot.instance,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                ));
            }
        }
        if state.business_identity.is_none() {
            state.business_identity = Some(identity);
            state.business_cancel = Some(cancel);
        }

        Ok(())
    }
    pub(super) fn binding(&self) -> &CarrierOwnerBinding {
        &self.binding
    }
    pub(super) fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    pub(super) fn upstream(&self) -> &UpstreamBinding {
        &self.upstream
    }
    pub(super) fn session(&self) -> SessionId {
        self.demand.session()
    }
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.demand.cancellation()
    }
    pub(super) fn check_liveness(&self) -> Result<(), ChannelError> {
        let Some(slot) = self.slot.upgrade() else {
            return Err(error(
                self.binding.registration().instance(),
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        };
        if self.session().0 == 0
            || self.binding.host_cancelled()
            || !self.binding.scope_identity().resource_open()
            || slot.cancel.is_cancelled()
            || self
                .caller
                .as_ref()
                .is_some_and(crate::contract::scope::TaskJoinWitness::is_joined)
            || self.cancellation().is_cancelled()
        {
            let mut failure = error(
                slot.instance,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            );
            failure.session = Some(self.session());
            return Err(failure);
        }
        self.demand.check()
    }
    pub(super) fn check(
        &self,
        grant: &CarrierGrant,
        binding: &UpstreamBinding,
        destination: &Destination,
        context: &CarrierContext,
    ) -> Result<(), ChannelError> {
        if !Weak::ptr_eq(&self.slot, &Arc::downgrade(&grant.slot))
            || !self.binding.registration().matches_grant(grant)
            || &self.upstream != binding
            || &self.endpoint != destination
            || context.session != Some(self.session())
            || context.deadline != self.demand.deadline()
        {
            let mut failure = error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            );
            failure.session = context.session;
            return Err(failure);
        }
        self.check_liveness()
    }
}
