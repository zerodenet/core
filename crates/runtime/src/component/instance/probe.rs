//! Same-host probe authority over the original Direct provider, not a second pool.
use super::*;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{network::UpstreamBinding, scope::TaskJoinWitness},
    controlled_carrier::{CarrierGrant, CarrierOwnerRegistration, CarrierServiceGrant},
};
impl ComponentSlot {
    pub fn check_probe_caller(&self, call: &crate::ProbeCall) -> Result<(), ChannelError> {
        if !self.has_started() {
            return Err(self.failure(Reason::Revoked, "probe Direct owner is not started"));
        }
        let host = self.admission.get().ok_or_else(|| {
            self.failure(Reason::StalePlan, "probe Direct owner is not installed")
        })?;
        host.check_probe(call)?;
        host.check_identity(
            self.instance_id(),
            &self.admission_owner,
            &zero_platform_tokio::contract::context::RequestIdentity::Service(call.identity()),
        )
    }
    pub fn check_direct_probe_owner(
        &self,
        call: &crate::ProbeCall,
        policy: &zero_traits::DialPolicy,
        actual: &CarrierOwnerRegistration,
        tasks: &zero_platform_tokio::contract::scope::TaskScopeIdentity,
    ) -> Result<(), ChannelError> {
        self.check_probe_caller(call)?;
        let live = self
            .live
            .get()
            .and_then(|live| live.as_ref().ok())
            .ok_or_else(|| {
                self.failure(Reason::Revoked, "probe Direct has no original provider")
            })?;
        let bound = live
            .resources
            .registered_direct_owner(self.instance_id(), policy, actual)?;
        if !bound
            .scope_identity()
            .same_scope(&live.tasks.scope_identity())
            || !tasks.same_scope(&live.tasks.scope_identity())
        {
            return Err(self.failure(
                Reason::StalePlan,
                "probe Direct provider has a foreign scope",
            ));
        }
        self.check_probe_caller(call)
    }
    pub fn issue_direct_probe_carrier(
        &self,
        call: &crate::ProbeCall,
        caller: TaskJoinWitness,
        endpoint: &Destination,
        binding: &UpstreamBinding,
        grant: &CarrierGrant,
    ) -> Result<CarrierServiceGrant, ChannelError> {
        self.check_probe_caller(call)?;
        let actual = call
            .witness()
            .ok_or_else(|| self.failure(Reason::Revoked, "probe original task missing"))?;
        if !actual.same_registration(&caller) && !grant.adoption().matches_auxiliary(&caller) {
            return Err(self.failure(
                Reason::StalePlan,
                "probe IO caller is not this original call or initializer",
            ));
        }
        let live = self
            .live
            .get()
            .and_then(|live| live.as_ref().ok())
            .ok_or_else(|| self.failure(Reason::Revoked, "probe original Direct was released"))?;
        let issuer = live.resources.registered_direct_service_issuer(
            self.instance_id(),
            endpoint,
            binding,
            grant,
            caller,
        )?;
        self.check_probe_caller(call)?;
        issuer.authorize(call.cancellation(), Some(call.deadline()))
    }
}
