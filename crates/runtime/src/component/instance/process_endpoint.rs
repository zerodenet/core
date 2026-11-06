//! Observe the actual live slot, not a claimed Process dependency ID or Scope.
use super::*;
use zero_platform_tokio::{
    contract::{component::ControlledStreamDemandClass, network::UpstreamBinding},
    controlled_carrier::CarrierGrant,
    owned_process::ProcessEndpointHandoff,
};
impl ComponentSlot {
    pub(in crate::component) fn ready_process_endpoint(
        &self,
        ordinal: usize,
    ) -> Result<Arc<ProcessEndpointHandoff>, ChannelError> {
        if !self.recipe.spec().roles.is_empty() || self.cancel.is_cancelled() {
            return Err(self.failure(
                Reason::Revoked,
                "derived endpoint requires its actual roleless Process",
            ));
        }
        if let Some(host) = self.admission.get() {
            host.check_installed(self.instance_id(), &self.admission_owner)?;
        }
        let Some(Ok(live)) = self.live.get() else {
            return Err(self.failure(Reason::Revoked, "actual Process owner is not installed"));
        };
        if !matches!(live.started.get(), Some(Ok(()))) {
            return Err(self.failure(Reason::Revoked, "actual Process owner is not started"));
        }
        live.resources.process_endpoint(ordinal).ok_or_else(|| {
            self.failure(
                Reason::StalePlan,
                "actual Process endpoint original is unavailable",
            )
        })
    }
    pub(in crate::component) fn endpoint_consumer_use(
        &self,
        handoff: &Arc<ProcessEndpointHandoff>,
        endpoint: &zero_core::request::Destination,
        binding: &UpstreamBinding,
        demand: ControlledStreamDemandClass,
        grant: &CarrierGrant,
    ) -> Result<zero_platform_tokio::owned_process::ProcessEndpointUse, ChannelError> {
        if self.cancel.is_cancelled() {
            return Err(self.failure(Reason::Revoked, "derived consumer stopped"));
        }
        if let Some(host) = self.admission.get() {
            host.check_installed(self.instance_id(), &self.admission_owner)?;
        }
        let Some(Ok(live)) = self.live.get() else {
            return Err(self.failure(Reason::Revoked, "derived consumer is not installed"));
        };
        if !matches!(live.started.get(), Some(Ok(()))) {
            return Err(self.failure(Reason::Revoked, "derived consumer is not started"));
        }
        live.resources.process_endpoint_consumer_use(
            self.instance_id(),
            handoff,
            endpoint,
            binding,
            demand,
            grant,
        )
    }
}
