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
    declaration: ResourceDeclaration,
    scope: TaskScopeIdentity,
    host_cancel: Arc<CancellationToken>,
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
        host_cancel: &Arc<CancellationToken>,
    ) -> Result<CarrierOwnerBinding, ChannelError> {
        let failure = |reason| {
            error(
                self.instance(),
                Stage::Prepare,
                reason,
                DeliveryState::NotSubmitted,
            )
        };
        let limit = match declaration {
            ResourceDeclaration::ControlledOutboundStream {
                endpoint,
                binding,
                max_sockets,
                ..
            } => {
                if endpoint.port == 0 {
                    return Err(failure(Reason::InvalidConfig));
                }
                if !binding.prefix.is_empty() || matches!(endpoint.address, Address::Domain(_)) {
                    return Err(failure(Reason::Unsupported));
                }
                *max_sockets
            }
            ResourceDeclaration::ControlledDirectStream { dial, max_sockets } => {
                dial.validate()
                    .map_err(|_| failure(Reason::InvalidConfig))?;
                *max_sockets
            }
            _ => return Err(failure(Reason::InvalidConfig)),
        };
        if limit != self.limit() {
            return Err(failure(Reason::InvalidConfig));
        }
        let scope = registrar.scope_identity();
        // No owner lock encloses the actual scope getter or any potential wake.
        if host_cancel.is_cancelled() || !scope.resource_open() {
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
            if !bound.matches(declaration, self.limit())
                || !bound.scope.matches(registrar)
                || !Arc::ptr_eq(&bound.host_cancel, host_cancel)
            {
                return Err(failure(Reason::StalePlan));
            }
            bound.clone()
        } else {
            let bound = Arc::new(BoundAuthority {
                declaration: declaration.clone(),
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
        &self.declaration == declaration
            && match declaration {
                ResourceDeclaration::ControlledOutboundStream { max_sockets, .. }
                | ResourceDeclaration::ControlledDirectStream { max_sockets, .. } => {
                    *max_sockets == limit
                }
                _ => false,
            }
    }
    fn matches_direct(&self, destination: &Destination, binding: &UpstreamBinding) -> bool {
        matches!(&self.declaration, ResourceDeclaration::ControlledDirectStream { dial, .. }
            if dial == &binding.dial && binding.prefix.is_empty() && destination.port != 0
            && !matches!(destination.address, Address::Domain(_)))
    }
    pub(super) fn permits_native(
        &self,
        binding: &UpstreamBinding,
        destination: &Destination,
        context: &CarrierContext,
    ) -> bool {
        let descriptor = match &self.declaration {
            ResourceDeclaration::ControlledOutboundStream {
                endpoint,
                binding: expected,
                demand,
                ..
            } => {
                expected == binding
                    && endpoint == destination
                    && match demand {
                        ControlledStreamDemandClass::Business => {
                            context.session.is_some_and(|id| id.0 != 0)
                        }
                        ControlledStreamDemandClass::Service
                        | ControlledStreamDemandClass::BackgroundControl => {
                            context.session.is_none()
                        }
                    }
            }
            ResourceDeclaration::ControlledDirectStream { .. } => {
                self.matches_direct(destination, binding)
                    && context.session.is_some_and(|id| id.0 != 0)
            }
            _ => false,
        };
        !self.host_cancel.is_cancelled() && self.scope.resource_open() && descriptor
    }
    pub(super) fn available(&self) -> bool {
        !self.host_cancel.is_cancelled() && self.scope.resource_open()
    }
    pub(super) fn host_cancelled(&self) -> bool {
        self.host_cancel.is_cancelled()
    }
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.host_cancel.as_ref().clone()
    }
    pub(super) fn permits_driver(&self, registrar: &TaskRegistrar) -> bool {
        !self.host_cancel.is_cancelled()
            && self.scope.matches(registrar)
            && self.scope.registration_open()
    }
}
impl CarrierOwnerBinding {
    pub(super) fn registration(&self) -> &CarrierOwnerRegistration {
        &self.registration
    }
    pub fn matches_host_cancel(&self, host_cancel: &Arc<CancellationToken>) -> bool {
        Arc::ptr_eq(&self.authority.host_cancel, host_cancel)
    }
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
    /// Dynamic Direct has no fabricated fixed endpoint or network generation.
    pub fn fixed_endpoint_binding(&self) -> Option<(&Destination, &UpstreamBinding)> {
        match &self.authority.declaration {
            ResourceDeclaration::ControlledOutboundStream {
                endpoint, binding, ..
            } => Some((endpoint, binding)),
            _ => None,
        }
    }
    pub fn matches_direct(&self, destination: &Destination, binding: &UpstreamBinding) -> bool {
        self.authority.matches_direct(destination, binding)
    }
    pub fn demand(&self) -> ControlledStreamDemandClass {
        match &self.authority.declaration {
            ResourceDeclaration::ControlledOutboundStream { demand, .. } => *demand,
            ResourceDeclaration::ControlledDirectStream { .. } => {
                ControlledStreamDemandClass::Business
            }
            _ => unreachable!("bound carrier declaration"),
        }
    }
    pub fn scope_identity(&self) -> TaskScopeIdentity {
        self.authority.scope.clone()
    }
}
