//! Bind the actual cold owner once; host registration remains separate authority.
use super::{error, CarrierOwnerRegistration};
use crate::contract::{
    component::{ControlledStreamDemandClass, ResourceDeclaration},
    network::{CarrierContext, UpstreamBinding},
    scope::{TaskRegistrar, TaskScopeIdentity},
    CancellationToken, ChannelError, DeliveryState, Reason, Stage,
};
use std::sync::Arc;
use zero_core::{request::Destination, Address};

pub(super) struct BoundAuthority {
    endpoint: Destination,
    binding: UpstreamBinding,
    demand: ControlledStreamDemandClass,
    scope: TaskScopeIdentity,
    host_cancel: CancellationToken,
}
/// Private-origin binding of one actual owner, full descriptor and actual scope.
/// This is registration evidence, not host business admission/native permission.
#[derive(Clone)]
pub struct CarrierOwnerBinding {
    registration: CarrierOwnerRegistration,
    authority: Arc<BoundAuthority>,
}
impl CarrierOwnerRegistration {
    pub fn bind_declared(
        &self,
        declaration: &ResourceDeclaration,
        registrar: &TaskRegistrar,
        host_cancel: &CancellationToken,
    ) -> Result<CarrierOwnerBinding, ChannelError> {
        let failure = |reason| {
            error(
                self.instance(),
                Stage::Prepare,
                reason,
                DeliveryState::NotSubmitted,
            )
        };
        let ResourceDeclaration::ControlledOutboundStream {
            endpoint,
            binding,
            demand,
            max_sockets,
        } = declaration
        else {
            return Err(failure(Reason::InvalidConfig));
        };
        if *max_sockets != self.limit() || endpoint.port == 0 {
            return Err(failure(Reason::InvalidConfig));
        }
        if !binding.prefix.is_empty() || matches!(endpoint.address, Address::Domain(_)) {
            return Err(failure(Reason::Unsupported));
        }
        let scope = registrar.scope_identity();
        // No owner lock encloses the actual scope getter or any potential wake.
        if host_cancel.is_cancelled() || !scope.registration_open() {
            return Err(failure(Reason::Revoked));
        }
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| failure(Reason::Revoked))?;
        let mut state = owner.state();
        if state.live || state.stopped || state.next != 0 || owner.cancel.is_cancelled() {
            return Err(failure(Reason::Revoked));
        }
        let authority = if let Some(bound) = &state.binding {
            if !bound.matches(declaration, self.limit()) || !bound.scope.matches(registrar) {
                return Err(failure(Reason::StalePlan));
            }
            bound.clone()
        } else {
            let bound = Arc::new(BoundAuthority {
                endpoint: endpoint.clone(),
                binding: binding.clone(),
                demand: *demand,
                scope,
                host_cancel: host_cancel.clone(),
            });
            state.binding = Some(bound.clone());
            bound
        };
        Ok(CarrierOwnerBinding {
            registration: self.clone(),
            authority,
        })
    }
    pub fn bound(&self) -> Option<CarrierOwnerBinding> {
        let owner = self.owner.upgrade()?;
        let authority = owner.state().binding.clone()?;
        Some(CarrierOwnerBinding {
            registration: self.clone(),
            authority,
        })
    }
}
impl BoundAuthority {
    fn matches(&self, declaration: &ResourceDeclaration, limit: std::num::NonZeroUsize) -> bool {
        matches!(declaration, ResourceDeclaration::ControlledOutboundStream { endpoint, binding, demand, max_sockets }
            if endpoint == &self.endpoint && binding == &self.binding && demand == &self.demand && *max_sockets == limit)
    }
    pub(super) fn permits_native(
        &self,
        binding: &UpstreamBinding,
        destination: &Destination,
        context: &CarrierContext,
    ) -> bool {
        !self.host_cancel.is_cancelled()
            && self.binding == *binding
            && self.endpoint == *destination
            && self.scope.registration_open()
            && match self.demand {
                ControlledStreamDemandClass::Business => {
                    context.session.is_some_and(|id| id.0 != 0)
                }
                ControlledStreamDemandClass::Service
                | ControlledStreamDemandClass::BackgroundControl => context.session.is_none(),
            }
    }
    pub(super) fn available(&self) -> bool {
        !self.host_cancel.is_cancelled() && self.scope.registration_open()
    }
    pub(super) fn host_cancelled(&self) -> bool {
        self.host_cancel.is_cancelled()
    }
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.host_cancel.clone()
    }
    pub(super) fn permits_driver(&self, registrar: &TaskRegistrar) -> bool {
        !self.host_cancel.is_cancelled()
            && self.scope.matches(registrar)
            && self.scope.registration_open()
    }
}
impl CarrierOwnerBinding {
    pub fn host_cancelled(&self) -> bool {
        self.authority.host_cancel.is_cancelled()
    }
    pub fn matches_registration(&self, registration: &CarrierOwnerRegistration) -> bool {
        self.registration.same_owner(registration)
    }
    pub fn matches_declaration(&self, declaration: &ResourceDeclaration) -> bool {
        self.authority
            .matches(declaration, self.registration.limit())
    }
    pub fn endpoint(&self) -> &Destination {
        &self.authority.endpoint
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.authority.binding
    }
    pub fn demand(&self) -> ControlledStreamDemandClass {
        self.authority.demand
    }
    pub fn scope_identity(&self) -> TaskScopeIdentity {
        self.authority.scope.clone()
    }
}
