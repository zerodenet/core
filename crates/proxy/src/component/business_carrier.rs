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
            })) { return Ok(None); }
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
    pub(super) fn authorize(
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
                .observe_carrier(grant.adoption())
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
