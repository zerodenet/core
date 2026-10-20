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
        };
        Ok(Some(request))
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
        slot.issue_business_carrier(
            endpoint,
            binding,
            grant,
            &self.identity,
            Arc::new(self.clone()),
        )
        .map_err(|mut error| {
            error.session.get_or_insert(self.session);
            error
        })
    }
}
impl CarrierBusinessDemand for BusinessCarrierRequest {
    fn check(&self) -> Result<(), ChannelError> {
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
