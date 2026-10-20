//! The concrete installed Slot is the only production business carrier issuer.
use super::*;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{context::RequestIdentity, network::UpstreamBinding},
    controlled_carrier::{CarrierBusinessDemand, CarrierBusinessGrant, CarrierGrant},
};
impl ComponentSlot {
    pub(in crate::component) fn check_business_carrier(
        &self,
        identity: &RequestIdentity,
    ) -> Result<(), ChannelError> {
        if !matches!(identity, RequestIdentity::Business(_)) || self.cancel.is_cancelled() {
            return Err(self.failure(Reason::Revoked, "business carrier owner is not available"));
        }
        let live = self
            .live
            .get()
            .and_then(|live| live.as_ref().ok())
            .ok_or_else(|| {
                self.failure(Reason::StalePlan, "business carrier owner has not built")
            })?;
        if !matches!(live.started.get(), Some(Ok(())))
            || !live.tasks.scope_identity().registration_open()
        {
            return Err(self.failure(
                Reason::Revoked,
                "business carrier owner has not successfully started",
            ));
        }
        self.admission
            .get()
            .ok_or_else(|| {
                self.failure(
                    Reason::StalePlan,
                    "business carrier owner has no installed host",
                )
            })?
            .check_identity(self.instance_id(), &self.admission_owner, identity)
    }
    pub(in crate::component) fn issue_business_carrier(
        &self,
        endpoint: &Destination,
        binding: &UpstreamBinding,
        grant: &CarrierGrant,
        identity: &RequestIdentity,
        demand: Arc<dyn CarrierBusinessDemand>,
    ) -> Result<CarrierBusinessGrant, ChannelError> {
        self.check_business_carrier(identity)?;
        let live = self
            .live
            .get()
            .and_then(|live| live.as_ref().ok())
            .ok_or_else(|| self.failure(Reason::Revoked, "business carrier owner was released"))?;
        let (registered, issuer) = live.resources.registered_business_issuer(
            self.instance_id(),
            endpoint,
            binding,
            grant,
        )?;
        if !registered
            .scope_identity()
            .same_scope(&live.tasks.scope_identity())
        {
            return Err(self.failure(
                Reason::StalePlan,
                "business carrier has a foreign host scope",
            ));
        }
        // Recheck after resource getters: they can flush actual I/O wakers.
        self.check_business_carrier(identity)?;
        issuer.authorize(demand)
    }
}
