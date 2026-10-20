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
}
#[derive(Clone)]
pub struct CarrierBusinessGrant {
    binding: CarrierOwnerBinding,
    slot: Weak<Slot>,
    demand: Arc<dyn CarrierBusinessDemand>,
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
        if self.demand() != ControlledStreamDemandClass::Business
            || !self.matches_host_cancel(host_fence)
            || self.host_cancelled()
            || !self.registration().matches_grant(grant)
            || !self.scope_identity().registration_open()
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
            caller,
        };
        grant.check_liveness()?;
        Ok(grant)
    }
}
impl CarrierBusinessGrant {
    pub(super) fn bind_caller(&self, slot: &Arc<Slot>) -> Result<(), ChannelError> {
        if let Some(caller) = &self.caller {
            let mut state = slot.state();
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
        Ok(())
    }
    pub(super) fn binding(&self) -> &CarrierOwnerBinding {
        &self.binding
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
            || !self.binding.scope_identity().registration_open()
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
            || self.binding.binding() != binding
            || self.binding.endpoint() != destination
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
