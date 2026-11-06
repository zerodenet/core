//! Same actual Node provider, selected direct Process dependency and original grant.
use super::*;
use zero_platform_tokio::{
    contract::{component::ControlledStreamDemandClass, network::ProcessEndpointStreamRequest},
    owned_process::ProcessEndpointDemand,
};
impl NodeOpener {
    pub(super) fn open_process_endpoint_stream(
        &self,
        binding: UpstreamBinding,
        request: ProcessEndpointStreamRequest,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        Box::pin(async move {
            let ProcessEndpointStreamRequest {
                process,
                process_ordinal,
                endpoint,
                context,
                grant,
            } = request;
            if !binding.prefix.is_empty()
                || binding.network_generation.0 != self.generation
                || self.services.network_generation() != self.generation
                || endpoint != self.endpoint
                || context.cancel.is_cancelled()
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "derived Process endpoint changed its actual Node binding",
                ));
            }
            let plan = self
                .plan
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
                .ok_or_else(|| invalid(Reason::Revoked, "actual Process consumer plan retired"))?;
            let ticket = if context.session.is_some() {
                Some(
                    self.business
                        .as_ref()
                        .ok_or_else(|| {
                            invalid(
                                Reason::Unsupported,
                                "derived Business requires actual admitted caller",
                            )
                        })?
                        .authorize(&endpoint, &binding, &grant)?,
                )
            } else {
                let invocation = self.service_call.as_ref().ok_or_else(|| {
                    invalid(
                        Reason::Unsupported,
                        "derived background requires actual Service invocation",
                    )
                })?;
                if invocation.session.is_some()
                    || invocation.deadline != context.deadline
                    || invocation.cancel.is_cancelled()
                {
                    return Err(invalid(
                        Reason::StalePlan,
                        "derived Service invocation changed",
                    ));
                }
                None
            };
            let class = if ticket.is_some() {
                ControlledStreamDemandClass::Business
            } else {
                ControlledStreamDemandClass::BackgroundControl
            };
            let derived = plan.process_endpoint_use(ProcessEndpointDemand {
                consumer: grant.instance(),
                process,
                process_ordinal,
                endpoint: &endpoint,
                binding: &binding,
                class,
                grant: &grant,
            })?;
            let native = match ticket {
                Some(ticket) => NativeCarrierOpener::business_process_endpoint(
                    ticket,
                    derived,
                    Arc::new(self.services.egress_control()),
                    self.services.observer(),
                )?,
                None => NativeCarrierOpener::background_process_endpoint(
                    &grant,
                    binding.clone(),
                    endpoint.clone(),
                    derived,
                    Arc::new(self.services.egress_control()),
                    self.services.observer(),
                )?,
            };
            let prepared = native
                .prepare_controlled_stream(&binding, endpoint, context.clone(), grant)?
                .with_endpoint_observer(Arc::new(ConnectedEvidence(self.evidence.clone())));
            drop(plan);
            Ok(prepared
                .open_with_deadline(context.deadline)
                .await?
                .into_parts()
                .0)
        })
    }
}
