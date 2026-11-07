//! Bind the actual cold owner once; host registration remains separate authority.
use super::{error, CarrierOwnerRegistration};
use crate::contract::{
    component::{ControlledStreamDemandClass, ResourceDeclaration},
    network::{CarrierContext, UpstreamBinding},
    scope::{TaskRegistrar, TaskScopeIdentity, WeakTaskRegistrar},
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
    pub(super) authority: Arc<BoundAuthority>,
}
impl CarrierOwnerRegistration {
    pub fn bind_declared(
        &self,
        declaration: &ResourceDeclaration,
        registrar: &TaskRegistrar,
        host_cancel: &Arc<CancellationToken>,
    ) -> Result<CarrierOwnerBinding, ChannelError> {
        self.bind_declared_inner(declaration, registrar, host_cancel, None)
    }
    /// The real host installs its one accounting root before admission can start.
    /// A root grants no native capability or lifecycle readiness.
    pub fn bind_declared_with_totals(
        &self,
        declaration: &ResourceDeclaration,
        registrar: &TaskRegistrar,
        host_cancel: &Arc<CancellationToken>,
        totals: &Arc<super::CarrierOriginalTotals>,
    ) -> Result<CarrierOwnerBinding, ChannelError> {
        self.bind_declared_inner(declaration, registrar, host_cancel, Some(totals))
    }
    fn bind_declared_inner(
        &self,
        declaration: &ResourceDeclaration,
        registrar: &TaskRegistrar,
        host_cancel: &Arc<CancellationToken>,
        totals: Option<&Arc<super::CarrierOriginalTotals>>,
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
            ResourceDeclaration::ControlledCarrierPool {
                endpoint,
                dial,
                max_sockets,
                ..
            }
            | ResourceDeclaration::ControlledServiceStream {
                endpoint,
                dial,
                max_sockets,
            }
            | ResourceDeclaration::ControlledAssociationPool {
                endpoint,
                dial,
                max_sockets,
            } => {
                if endpoint.port == 0 {
                    return Err(failure(Reason::InvalidConfig));
                }
                dial.validate()
                    .map_err(|_| failure(Reason::InvalidConfig))?;
                *max_sockets
            }
            ResourceDeclaration::ControlledOutboundStream {
                endpoint,
                binding,
                max_sockets,
                ..
            } => {
                if endpoint.port == 0 {
                    return Err(failure(Reason::InvalidConfig));
                }
                if !binding.prefix.is_empty() {
                    return Err(failure(Reason::Unsupported));
                }
                *max_sockets
            }
            ResourceDeclaration::ControlledDatagramCarrier {
                endpoint,
                binding,
                max_sockets,
            } => {
                if endpoint.port == 0 {
                    return Err(failure(Reason::InvalidConfig));
                }
                if !binding.prefix.is_empty() {
                    return Err(failure(Reason::Unsupported));
                }
                *max_sockets
            }
            ResourceDeclaration::ControlledNodeStream {
                endpoint,
                dial,
                max_sockets,
            }
            | ResourceDeclaration::ControlledNodeCarrier {
                endpoint,
                dial,
                max_sockets,
            } => {
                if endpoint.port == 0 {
                    return Err(failure(Reason::InvalidConfig));
                }
                dial.validate()
                    .map_err(|_| failure(Reason::InvalidConfig))?;
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
        if totals.is_some_and(|totals| {
            owner
                .totals
                .get()
                .is_some_and(|actual| !Arc::ptr_eq(actual, totals))
        }) {
            return Err(failure(Reason::StalePlan));
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
        if let Some(totals) = totals {
            // Owner state serializes bind/start/reserve, including repeated binds.
            let _ = owner.totals.set(totals.clone());
        }
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
    pub(super) fn is_stream(&self) -> bool {
        matches!(
            self.declaration,
            ResourceDeclaration::ControlledOutboundStream { .. }
                | ResourceDeclaration::ControlledAssociationPool { .. }
                | ResourceDeclaration::ControlledCarrierPool {
                    datagram: false,
                    ..
                }
                | ResourceDeclaration::ControlledDirectStream { .. }
                | ResourceDeclaration::ControlledNodeCarrier { .. }
                | ResourceDeclaration::ControlledNodeStream { .. }
                | ResourceDeclaration::ControlledServiceStream { .. }
        )
    }
    pub(super) fn background_custody_available(&self) -> bool {
        self.available()
            && matches!(
                &self.declaration,
                ResourceDeclaration::ControlledOutboundStream {
                    demand: ControlledStreamDemandClass::BackgroundControl,
                    ..
                } | ResourceDeclaration::ControlledDatagramCarrier { .. }
                    | ResourceDeclaration::ControlledCarrierPool { .. }
                    | ResourceDeclaration::ControlledServiceStream { .. }
                    | ResourceDeclaration::ControlledAssociationPool { .. }
            )
    }
    pub(super) fn preparation_available(&self) -> bool {
        self.available()
            && matches!(
                self.declaration,
                ResourceDeclaration::ControlledNodeCarrier { .. }
                    | ResourceDeclaration::ControlledNodeStream { .. }
                    | ResourceDeclaration::ControlledServiceStream { .. }
            )
    }
    pub(super) fn service_candidates_available(&self) -> bool {
        self.background_custody_available()
            || (self.available()
                && matches!(
                    self.declaration,
                    ResourceDeclaration::ControlledNodeCarrier { .. }
                ))
    }
    pub(super) fn transferred_custody_available(&self) -> bool {
        self.background_custody_available()
            || (self.available()
                && matches!(
                    self.declaration,
                    ResourceDeclaration::ControlledNodeStream { .. }
                        | ResourceDeclaration::ControlledNodeCarrier { .. }
                ))
    }
    pub(super) fn matches(
        &self,
        declaration: &ResourceDeclaration,
        limit: std::num::NonZeroUsize,
    ) -> bool {
        &self.declaration == declaration
            && match declaration {
                ResourceDeclaration::ControlledOutboundStream { max_sockets, .. }
                | ResourceDeclaration::ControlledCarrierPool { max_sockets, .. }
                | ResourceDeclaration::ControlledServiceStream { max_sockets, .. }
                | ResourceDeclaration::ControlledAssociationPool { max_sockets, .. }
                | ResourceDeclaration::ControlledDatagramCarrier { max_sockets, .. }
                | ResourceDeclaration::ControlledDirectStream { max_sockets, .. } => {
                    *max_sockets == limit
                }
                ResourceDeclaration::ControlledNodeStream { max_sockets, .. }
                | ResourceDeclaration::ControlledNodeCarrier { max_sockets, .. } => {
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
    fn matches_node(&self, destination: &Destination, binding: &UpstreamBinding) -> bool {
        matches!(&self.declaration, ResourceDeclaration::ControlledNodeStream { endpoint, dial, .. }
            | ResourceDeclaration::ControlledNodeCarrier { endpoint, dial, .. }
            | ResourceDeclaration::ControlledAssociationPool { endpoint, dial, .. }
            if endpoint == destination && dial == &binding.dial && binding.prefix.is_empty())
    }
    pub(super) fn permits_native(
        &self,
        binding: &UpstreamBinding,
        destination: &Destination,
        context: &CarrierContext,
    ) -> bool {
        let descriptor = match &self.declaration {
            ResourceDeclaration::ControlledServiceStream { endpoint, dial, .. } => {
                endpoint == destination
                    && dial == &binding.dial
                    && binding.prefix.is_empty()
                    && context.session.is_none()
            }
            ResourceDeclaration::ControlledCarrierPool { endpoint, dial, .. } => {
                endpoint == destination && dial == &binding.dial && context.session.is_none()
            }
            ResourceDeclaration::ControlledAssociationPool { endpoint, dial, .. } => {
                // None keeps the same original Service/BND authority. Some
                // still requires NativeCarrierOpener's sealed Business ticket
                // and its actual bound caller before any native preparation.
                endpoint == destination
                    && dial == &binding.dial
                    && context.session.is_none_or(|session| session.0 != 0)
            }
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
            ResourceDeclaration::ControlledNodeCarrier { .. } => {
                // NativeCarrierOpener independently requires the original sealed
                // Business grant whenever session is Some; None is actual
                // transferred taskless Service custody, never a Business ticket.
                self.matches_node(destination, binding)
                    && context.session.is_none_or(|id| id.0 != 0)
            }
            ResourceDeclaration::ControlledNodeStream { .. } => {
                self.matches_node(destination, binding)
                    && context.session.is_some_and(|id| id.0 != 0)
            }
            ResourceDeclaration::ControlledDatagramCarrier {
                endpoint,
                binding: expected,
                ..
            } => expected == binding && endpoint == destination && context.session.is_none(),
            _ => false,
        };
        !self.host_cancel.is_cancelled() && self.scope.resource_open() && descriptor
    }
    // Borrowed custody is not native acquisition. A per-connection Service
    // may adopt its actual strictly shorter prefix without granting root I/O
    // or cached-driver handoff to this declaration.
    pub(super) fn permits_service_stream_loan(
        &self,
        binding: &UpstreamBinding,
        destination: &Destination,
        context: &CarrierContext,
    ) -> bool {
        self.available()
            && context.session.is_none()
            && !binding.prefix.is_empty()
            && matches!(&self.declaration, ResourceDeclaration::ControlledServiceStream { endpoint, dial, .. }
                if endpoint == destination && dial == &binding.dial)
    }
    pub(super) fn available(&self) -> bool {
        !self.host_cancel.is_cancelled() && self.scope.resource_open()
    }
    pub(super) fn pool_kind(&self) -> Option<bool> {
        match self.declaration {
            ResourceDeclaration::ControlledCarrierPool { datagram, .. } => Some(datagram),
            ResourceDeclaration::ControlledAssociationPool { .. }
            | ResourceDeclaration::ControlledServiceStream { .. }
            | ResourceDeclaration::ControlledNodeCarrier { .. } => Some(false),
            _ => None,
        }
    }
    pub(super) fn permits_related(&self) -> bool {
        self.available()
            && matches!(
                self.declaration,
                ResourceDeclaration::ControlledAssociationPool { .. }
            )
    }
    pub(super) fn host_cancelled(&self) -> bool {
        self.host_cancel.is_cancelled()
    }
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.host_cancel.as_ref().clone()
    }
    pub(super) fn permits_auxiliary(&self, registrar: &WeakTaskRegistrar) -> bool {
        !self.host_cancel.is_cancelled()
            && self.scope.same_scope(&registrar.scope_identity())
            && self.scope.registration_open()
    }
    pub(super) fn retains_driver(&self, witness: &crate::contract::scope::TaskJoinWitness) -> bool {
        self.pool_kind().is_some()
            && !matches!(
                self.declaration,
                ResourceDeclaration::ControlledServiceStream { .. }
                    | ResourceDeclaration::ControlledNodeCarrier { .. }
                    | ResourceDeclaration::ControlledAssociationPool { .. }
            )
            && !self.host_cancel.is_cancelled()
            && self.scope.resource_open()
            && self.scope.same_scope(&witness.scope_identity())
    }
    pub(super) fn permits_driver(&self, registrar: &TaskRegistrar) -> bool {
        // This checks the original binding/scope, not available task credit.
        // The same real registrar must perform the tracked registration and
        // return its Capacity/Closing source, including a zero-task scope.
        !matches!(
            self.declaration,
            ResourceDeclaration::ControlledNodeCarrier { .. }
        ) && !self.host_cancel.is_cancelled()
            && self.scope.matches(registrar)
            && self.scope.resource_open()
    }
}
impl CarrierOwnerBinding {
    pub(super) fn matches_authority(&self, actual: &Arc<BoundAuthority>) -> bool {
        Arc::ptr_eq(&self.authority, actual)
    }
    pub(crate) fn registration(&self) -> &CarrierOwnerRegistration {
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
            }
            | ResourceDeclaration::ControlledDatagramCarrier {
                endpoint, binding, ..
            } => Some((endpoint, binding)),
            _ => None,
        }
    }
    pub fn matches_direct(&self, destination: &Destination, binding: &UpstreamBinding) -> bool {
        self.authority.matches_direct(destination, binding)
    }
    pub fn matches_node(&self, destination: &Destination, binding: &UpstreamBinding) -> bool {
        self.authority.matches_node(destination, binding)
    }
    /// Descriptor eligibility only. This never authorizes a Business session;
    /// the host issuer still validates its exact original grant/caller/fence.
    /// Read only the registered aggregate's exact UDP endpoint/dial facts.
    /// This mints neither Process use, Service caller nor native permission.
    pub fn matches_background_datagram_pool(
        &self,
        endpoint: &Destination,
        binding: &UpstreamBinding,
    ) -> bool {
        binding.prefix.is_empty()
            && matches!(&self.authority.declaration,ResourceDeclaration::ControlledCarrierPool{endpoint:actual,dial,datagram:true,..} if actual==endpoint&&dial==&binding.dial)
    }
    pub fn supports_business(&self) -> bool {
        self.demand() == ControlledStreamDemandClass::Business
            || matches!(
                self.authority.declaration,
                ResourceDeclaration::ControlledAssociationPool { .. }
            )
    }
    pub fn demand(&self) -> ControlledStreamDemandClass {
        match &self.authority.declaration {
            ResourceDeclaration::ControlledOutboundStream { demand, .. } => *demand,
            ResourceDeclaration::ControlledDirectStream { .. }
            | ResourceDeclaration::ControlledNodeStream { .. }
            | ResourceDeclaration::ControlledNodeCarrier { .. } => {
                ControlledStreamDemandClass::Business
            }
            ResourceDeclaration::ControlledDatagramCarrier { .. } => {
                ControlledStreamDemandClass::BackgroundControl
            }
            ResourceDeclaration::ControlledCarrierPool { .. }
            | ResourceDeclaration::ControlledServiceStream { .. }
            | ResourceDeclaration::ControlledAssociationPool { .. } => {
                ControlledStreamDemandClass::BackgroundControl
            }
            _ => unreachable!("bound carrier declaration"),
        }
    }
    pub fn scope_identity(&self) -> TaskScopeIdentity {
        self.authority.scope.clone()
    }
}
