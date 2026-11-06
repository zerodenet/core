//! Re-select the same real root for a separately owned Service demand.
//! No grant, observer or original caller is manufactured by this metadata step.
use super::*;
use zero_platform_tokio::contract::network::UpstreamTransport;
impl NodeOpener {
    pub(in crate::component::stream_bridge) fn reselect_service(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        Box::pin(async move {
            let binding = binding.ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "Service root requires its exact selected binding",
                )
            })?;
            if !binding.prefix.is_empty()
                || call.session.is_some()
                || call.cancel.is_cancelled()
                || binding.network_generation.0 != self.generation
                || self.services.network_generation() != self.generation
                || self
                    .pending
                    .as_ref()
                    .is_some_and(|(actual, _)| actual != &binding)
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "Service root selection changed its original binding",
                ));
            }
            // An actual Business prefix is admitted before selecting a distinct
            // Service donor. Its facts/ticket never become that donor's identity,
            // cancellation or native authority. Native prepare still requires
            // the donor's own installed finite owner and actual grant.
            if let Some(business) = &self.business {
                zero_platform_tokio::controlled_carrier::CarrierBusinessDemand::check(business)?;
            } else if self.service_call.is_none() {
                return Err(invalid(
                    Reason::Unsupported,
                    "root has no actual owning invocation",
                ));
            }
            let mut selected = NodeOpener::new(self.services.clone(), self.endpoint.clone())
                .with_plan(self.plan.clone())
                .with_pending(binding.clone(), waiting)
                .with_service_call(call);
            selected.carrier_owner = self.carrier_owner;
            Ok(UpstreamTransport::new(binding, Arc::new(selected)))
        })
    }
}
