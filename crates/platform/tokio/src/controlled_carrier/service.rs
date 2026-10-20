//! Original host-fence authority for one fixed Service slot and real caller.
use super::{error, slot::Slot, CarrierGrant, CarrierOwnerBinding};
use crate::contract::{
    component::ControlledStreamDemandClass,
    network::{CarrierContext, UpstreamBinding},
    scope::TaskJoinWitness,
    CancellationToken, ChannelError, DeliveryState, Reason, Stage,
};
use std::sync::{Arc, Weak};
use tokio::time::Instant;
use zero_core::request::Destination;

/// Keep in the actual host after its original root authority/query checks.
/// Public Service metadata or a caller-created fence cannot construct this for
/// a different registered original owner. It issues no business SessionId.
pub struct CarrierServiceIssuer {
    binding: CarrierOwnerBinding,
    slot: Weak<Slot>,
    caller: TaskJoinWitness,
}
#[derive(Clone)]
pub struct CarrierServiceGrant {
    binding: CarrierOwnerBinding,
    slot: Weak<Slot>,
    demand: Arc<ServiceDemand>,
}
pub(super) struct ServiceDemand {
    caller: TaskJoinWitness,
    cancel: CancellationToken,
    deadline: Option<Instant>,
}
impl CarrierOwnerBinding {
    pub fn service_issuer(
        &self,
        original_host_fence: &Arc<CancellationToken>,
        grant: &CarrierGrant,
        caller: TaskJoinWitness,
    ) -> Result<CarrierServiceIssuer, ChannelError> {
        grant.check()?;
        if self.demand() != ControlledStreamDemandClass::Service
            || self.fixed_endpoint_binding().is_none()
            || !self.matches_host_cancel(original_host_fence)
            || !self.registration().matches_grant(grant)
            || !self.scope_identity().same_scope(&caller.scope_identity())
            || !self.scope_identity().resource_open()
            || self.host_cancelled()
            || caller.is_joined()
        {
            return Err(failure(grant.instance(), Reason::StalePlan));
        }
        Ok(CarrierServiceIssuer {
            binding: self.clone(),
            slot: Arc::downgrade(&grant.slot),
            caller,
        })
    }
}
impl CarrierServiceIssuer {
    /// The actual root provider checks its private root identity before issuing.
    /// The unique same-slot user lease must already exist. Bind the original
    /// caller durably before this ticket can reach native preparation.
    pub fn authorize(
        self,
        query_cancel: CancellationToken,
        query_deadline: Option<Instant>,
    ) -> Result<CarrierServiceGrant, ChannelError> {
        let ticket = CarrierServiceGrant {
            binding: self.binding,
            slot: self.slot,
            demand: Arc::new(ServiceDemand {
                caller: self.caller,
                cancel: query_cancel,
                deadline: query_deadline,
            }),
        };
        ticket.check_liveness()?;
        let slot = ticket
            .slot
            .upgrade()
            .ok_or_else(|| failure(ticket.binding.registration().instance(), Reason::Revoked))?;
        ticket.bind_caller(&slot)?;
        Ok(ticket)
    }
}
impl CarrierServiceGrant {
    pub(super) fn binding(&self) -> &CarrierOwnerBinding {
        &self.binding
    }
    pub(super) fn endpoint(&self) -> &Destination {
        self.binding
            .fixed_endpoint_binding()
            .expect("fixed Service ticket")
            .0
    }
    pub(super) fn upstream(&self) -> &UpstreamBinding {
        self.binding
            .fixed_endpoint_binding()
            .expect("fixed Service ticket")
            .1
    }
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.demand.cancel.clone()
    }
    pub(super) fn check_liveness(&self) -> Result<(), ChannelError> {
        let slot = self
            .slot
            .upgrade()
            .ok_or_else(|| failure(self.binding.registration().instance(), Reason::Revoked))?;
        let closed = slot.state().closed;
        if closed
            || self.binding.demand() != ControlledStreamDemandClass::Service
            || !self
                .binding
                .registration()
                .matches_adoption(&super::CarrierAdoption { slot: slot.clone() })
            || !self
                .binding
                .scope_identity()
                .same_scope(&self.demand.caller.scope_identity())
            || !self.binding.scope_identity().resource_open()
            || self.binding.host_cancelled()
            || slot.cancel.is_cancelled()
            || self.demand.caller.is_joined()
            || self.demand.cancel.is_cancelled()
        {
            return Err(failure(slot.instance, Reason::Revoked));
        }
        if self
            .demand
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(failure(slot.instance, Reason::Timeout));
        }
        Ok(())
    }
    pub(super) fn bind_caller(&self, slot: &Arc<Slot>) -> Result<(), ChannelError> {
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
        if !state
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
    pub(super) fn check(
        &self,
        grant: &CarrierGrant,
        binding: &UpstreamBinding,
        destination: &Destination,
        context: &CarrierContext,
    ) -> Result<(), ChannelError> {
        if !Weak::ptr_eq(&self.slot, &Arc::downgrade(&grant.slot))
            || !self.binding.registration().matches_grant(grant)
            || self.upstream() != binding
            || self.endpoint() != destination
            || context.session.is_some()
            || context.deadline != self.demand.deadline
        {
            return Err(failure(grant.instance(), Reason::StalePlan));
        }
        self.check_liveness()
    }
}
fn failure(instance: zero_core::component::ComponentInstanceId, reason: Reason) -> ChannelError {
    error(
        instance,
        Stage::Prepare,
        reason,
        DeliveryState::NotSubmitted,
    )
}
