//! Actual ingress admission and current host owner, never public identity authority.
use super::{
    business_demand::AdmittedTcpDemand,
    direct_bridge::Invocation,
    instance::{invalid, ComponentSlot},
};
use std::sync::{Arc, Weak};
use tokio::time::Instant;
use zero_core::{component::SessionId, request::Destination};
use zero_engine::Engine;
use zero_platform_tokio::{
    contract::{
        context::RequestIdentity, network::UpstreamBinding, CancellationToken, ChannelError, Reason,
    },
    controlled_carrier::{CarrierBusinessDemand, CarrierBusinessGrant, CarrierGrant},
};

/// Constructible only inside the proxy from the actual returned Engine receipt.
/// Its retained projection holds no Engine, configuration or upstream opener.
#[derive(Clone)]
pub(super) struct BusinessCarrierRequest {
    slot: Weak<ComponentSlot>,
    admitted: AdmittedTcpDemand,
    identity: RequestIdentity,
    session: SessionId,
    deadline: Option<Instant>,
    direct: bool,
    caller_accepted: Option<zero_platform_tokio::AcceptedTcpCaller>,
    #[cfg(feature = "raw-ip-runtime")]
    caller_native:
        Option<crate::runtime::inbound_operation::raw_ip::native_obligations::NativeRegistration>,
}
impl BusinessCarrierRequest {
    pub(super) fn new(
        engine: &Engine,
        slot: &Arc<ComponentSlot>,
        invocation: &Invocation,
    ) -> Result<Option<Self>, ChannelError> {
        // Ordinary role opens retain their existing path. A retained request
        // projection is useful only for an explicitly declared Business owner.
        if !slot.recipe.spec().resources.iter().any(|resource| matches!(resource,
            zero_platform_tokio::contract::component::ResourceDeclaration::ControlledOutboundStream {
                demand: zero_platform_tokio::contract::component::ControlledStreamDemandClass::Business, ..
            } | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream { .. })) { return Ok(None); }
        let Some(admitted) = &invocation.admitted else {
            return Ok(None);
        };
        let RequestIdentity::Business(facts) = &invocation.identity else {
            return Err(invalid(
                Reason::InvalidConfig,
                "Engine admission requires original business facts",
            ));
        };
        admitted
            .check_live(engine, facts.session)
            .map_err(|source| {
                let mut error = invalid(
                    Reason::Revoked,
                    "original Engine business admission is no longer active",
                )
                .with_source(source);
                error.session = Some(facts.session);
                error
            })?;
        let request = Self {
            slot: Arc::downgrade(slot),
            admitted: admitted.clone(),
            identity: invocation.identity.clone(),
            session: facts.session,
            deadline: invocation.deadline,
            direct: slot.recipe.spec().resources.iter().any(|resource| matches!(resource, zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream { .. })),
            caller_accepted: invocation.caller_accepted.clone(),
            #[cfg(feature = "raw-ip-runtime")]
            caller_native: invocation.caller_native.clone(),
        };
        Ok(Some(request))
    }
    fn caller_witness(
        &self,
    ) -> Result<Option<zero_platform_tokio::contract::scope::TaskJoinWitness>, ChannelError> {
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(caller) = &self.caller_native {
            caller.check_open().map_err(|mut error| {
                error.session.get_or_insert(self.session);
                error
            })?;
            return caller.caller_witness().map(Some).ok_or_else(|| {
                invalid(
                    Reason::Revoked,
                    "actual caller has no original tracked registration",
                )
            });
        }
        if let Some(caller) = &self.caller_accepted {
            caller.check_open().map_err(|mut error| {
                error.session.get_or_insert(self.session);
                error
            })?;
            return caller.task_join_witness().map(Some).ok_or_else(|| {
                invalid(
                    Reason::Revoked,
                    "accepted caller has no original tracked registration",
                )
            });
        }
        Ok(None)
    }
    pub(super) fn has_tracked_caller(&self) -> bool {
        self.caller_witness().is_ok_and(|caller| caller.is_some())
    }
    pub(super) fn check_direct_destination(
        &self,
        destination: &Destination,
    ) -> Result<(), ChannelError> {
        if !self.direct
            || !matches!(&self.identity, RequestIdentity::Business(facts) if facts.target.as_ref() == Some(destination))
        {
            return Err(invalid(
                Reason::StalePlan,
                "Direct request differs from original admitted destination",
            ));
        }
        self.check()
    }
    pub(super) fn check_direct_owner(
        &self,
        policy: &zero_traits::DialPolicy,
        registration: &zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration,
        tasks: &zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    ) -> Result<(), ChannelError> {
        self.check()?;
        if !self.direct || self.caller_witness()?.is_none() {
            return Err(invalid(
                Reason::Unsupported,
                "controlled Direct requires its actual tracked caller",
            ));
        }
        let slot = self
            .slot
            .upgrade()
            .ok_or_else(|| invalid(Reason::Revoked, "original Direct owner released"))?;
        slot.check_direct_carrier_owner(
            &self.identity,
            policy,
            registration,
            &tasks.scope_identity(),
        )
    }
    pub(super) fn authorize_direct(
        &self,
        candidate: &crate::transport::DirectCandidate,
        binding: &UpstreamBinding,
        grant: &CarrierGrant,
    ) -> Result<CarrierBusinessGrant, ChannelError> {
        if !self.direct || candidate.generation() != binding.network_generation {
            return Err(invalid(
                Reason::StalePlan,
                "Direct candidate lacks its original selected generation",
            ));
        }
        let peer = candidate.peer();
        let address = match peer.ip() {
            std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
        };
        self.authorize_selected(&Destination::new(address, peer.port()), binding, grant)
    }
    pub(super) fn authorize(
        &self,
        endpoint: &Destination,
        binding: &UpstreamBinding,
        grant: &CarrierGrant,
    ) -> Result<CarrierBusinessGrant, ChannelError> {
        if self.direct {
            return Err(invalid(
                Reason::Unsupported,
                "Direct business capability requires an original resolved candidate",
            ));
        }
        self.authorize_selected(endpoint, binding, grant)
    }
    fn authorize_selected(
        &self,
        endpoint: &Destination,
        binding: &UpstreamBinding,
        grant: &CarrierGrant,
    ) -> Result<CarrierBusinessGrant, ChannelError> {
        self.check()?;
        let slot = self
            .slot
            .upgrade()
            .ok_or_else(|| invalid(Reason::Revoked, "original component owner was released"))?;
        let caller = self.caller_witness()?;
        let authorized = slot
            .issue_business_carrier(
                endpoint,
                binding,
                grant,
                &self.identity,
                Arc::new(self.clone()),
                caller,
            )
            .map_err(|mut error| {
                error.session.get_or_insert(self.session);
                error
            })?;
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(caller) = &self.caller_native {
            // The host issuer has already installed this original caller on
            // the same reservation. Observe it before native preparation/await;
            // the original raw owner alone decides its remaining obligations.
            caller
                .observe_carrier_with_owner(&authorized.owner_registration(), grant.adoption())
                .map_err(|mut error| {
                    error.session.get_or_insert(self.session);
                    error
                })?;
        }
        Ok(authorized)
    }
}
impl CarrierBusinessDemand for BusinessCarrierRequest {
    fn check(&self) -> Result<(), ChannelError> {
        if self.admitted.cancellation().is_cancelled() {
            let mut error = invalid(
                Reason::Revoked,
                "original business demand cancellation was revoked",
            );
            error.session = Some(self.session);
            return Err(error);
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(caller) = &self.caller_native {
            caller.check_open().map_err(|mut error| {
                error.session.get_or_insert(self.session);
                error
            })?;
        }
        if let Some(caller) = &self.caller_accepted {
            caller.check_open().map_err(|mut error| {
                error.session.get_or_insert(self.session);
                error
            })?;
        }
        self.admitted.check_active().map_err(|source| {
            let mut error = invalid(Reason::Revoked, "original Engine business admission ended")
                .with_source(source);
            error.session = Some(self.session);
            error
        })?;
        let slot = self
            .slot
            .upgrade()
            .ok_or_else(|| invalid(Reason::Revoked, "original component owner was released"))?;
        slot.check_business_carrier(&self.identity)
            .map_err(|mut error| {
                error.session.get_or_insert(self.session);
                error
            })
    }
    fn session(&self) -> SessionId {
        self.session
    }
    fn cancellation(&self) -> CancellationToken {
        self.admitted.cancellation()
    }
    fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}
