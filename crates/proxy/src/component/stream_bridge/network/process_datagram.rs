//! Aggregate UDP has its own Service grant; it never borrows the first Session.
use super::*;
use zero_platform_tokio::{
    contract::{component::ControlledStreamDemandClass, network::ProcessEndpointDatagramRequest},
    owned_process::ProcessEndpointDemand,
};
impl NodeOpener {
    pub(super) fn open_process_endpoint_datagram(
        &self,
        binding: UpstreamBinding,
        request: ProcessEndpointDatagramRequest,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierDatagram, ChannelError>>
    {
        Box::pin(async move {
            let ProcessEndpointDatagramRequest {
                process,
                process_ordinal,
                endpoint,
                context,
                grant,
                startup_deadline,
            } = request;
            if context.session.is_some()
                || !binding.prefix.is_empty()
                || binding.network_generation.0 != self.generation
                || self.services.network_generation() != self.generation
                || endpoint != self.endpoint
                || context.cancel.is_cancelled()
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "derived aggregate UDP changed its actual Service binding",
                ));
            }
            let invocation = self.service_call.as_ref().ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "aggregate UDP requires its actual Service invocation",
                )
            })?;
            if invocation.session.is_some()
                || invocation.deadline != context.deadline
                || invocation.cancel.is_cancelled()
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "aggregate UDP Service invocation changed",
                ));
            }
            let plan = self
                .plan
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
                .ok_or_else(|| {
                    invalid(
                        Reason::Revoked,
                        "actual aggregate Process consumer plan retired",
                    )
                })?;
            let startup_deadline = context
                .deadline
                .map_or(startup_deadline, |end| startup_deadline.min(end));
            let derived = plan
                .ensure_process_endpoint_use(
                    ProcessEndpointDemand {
                        consumer: grant.instance(),
                        process,
                        process_ordinal,
                        endpoint: &endpoint,
                        binding: &binding,
                        class: ControlledStreamDemandClass::BackgroundControl,
                        grant: &grant,
                    },
                    startup_deadline,
                    &context.cancel,
                )
                .await?;
            let native = NativeCarrierOpener::background_process_endpoint(
                &grant,
                binding.clone(),
                endpoint.clone(),
                derived,
                Arc::new(self.services.egress_control()),
                self.services.observer(),
            )?;
            // packet() independently checks session=None, exact actual owner /
            // declaration, grant, epoch, derived fence and native disposition.
            let datagram =
                native.prepare_controlled_datagram(&binding, endpoint, context, grant)?;
            drop(plan);
            Ok(datagram)
        })
    }
}
