//! Derived loopback is authorized by its actual Process dependency, not DNS IO.
use super::*;
use crate::component::stream_bridge::network::NodeOpener;
use zero_platform_tokio::{
    contract::network::{ProcessEndpointDatagramRequest, ProcessEndpointStreamRequest},
    controlled_carrier::{CarrierDatagram, CarrierGrant, CarrierIo},
};
impl Root {
    fn process_opener(
        &self,
        binding: &UpstreamBinding,
        endpoint: &Destination,
        call: &CarrierContext,
        grant: &CarrierGrant,
    ) -> Result<NodeOpener, ChannelError> {
        let failure = || {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "derived Process endpoint differs from its actual DNS Service invocation",
            )
        };
        if call.session.is_some()
            || self.invocation.session.is_some()
            || call.deadline != self.invocation.deadline
            || call.cancel.is_cancelled()
            || self.invocation.cancel.is_cancelled()
        {
            return Err(failure());
        }
        // Persist the exact consumer's cancellation/native controller before
        // process startup or any derived connect may be polled. It remains in
        // the original query's finalizer on failure or waiter cancellation.
        self.retain_pending_carrier(binding, grant.adoption())?;
        let source = self.current.upgrade().ok_or_else(failure)?;
        let services = source.runtime_services().map_err(|_| failure())?;
        let execution = services.execution();
        let plan = execution.components.as_ref().map_err(|_| failure())?;
        if !self.roles.iter().any(|role| {
            plan.validate_carrier_adoption(*role, &grant.adoption())
                .is_ok()
        }) {
            return Err(failure());
        }
        let waiting = self.waiting.get().cloned().ok_or_else(failure)?;
        Ok(NodeOpener::new(
            execution.upstream().with_observer(self.observer.clone()),
            endpoint.clone(),
        )
        .with_plan(Some(Arc::downgrade(plan)))
        .with_service_call(self.invocation.clone())
        .with_pending(binding.clone(), Some(waiting)))
    }

    pub(super) fn process_stream(
        &self,
        binding: UpstreamBinding,
        request: ProcessEndpointStreamRequest,
    ) -> BoxFuture<'_, Result<CarrierIo, ChannelError>> {
        Box::pin(async move {
            let opener = self.process_opener(
                &binding,
                &request.endpoint,
                &request.context,
                &request.grant,
            )?;
            opener
                .acquire_process_endpoint_stream(&binding, request)
                .await
        })
    }

    pub(super) fn process_datagram(
        &self,
        binding: UpstreamBinding,
        request: ProcessEndpointDatagramRequest,
    ) -> BoxFuture<'_, Result<CarrierDatagram, ChannelError>> {
        Box::pin(async move {
            let opener = self.process_opener(
                &binding,
                &request.endpoint,
                &request.context,
                &request.grant,
            )?;
            opener
                .acquire_process_endpoint_datagram(&binding, request)
                .await
        })
    }
}
