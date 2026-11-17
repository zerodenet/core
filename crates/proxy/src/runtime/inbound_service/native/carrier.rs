//! Selection borrows current execution inputs for one call. No retained Engine,
//! configuration snapshot, native constructor, private pool or fallback opener.
use super::*;
use crate::{inventory::service::ServiceNode, runtime::tcp_dispatch::service};
use zero_core::{component::NetworkGeneration, request::Destination};
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    stream::BoxStream,
    BoxFuture, ChannelError, Reason,
};
struct Opener {
    source: std::sync::Weak<crate::runtime::current_services::CurrentServiceSource>,
    donor: ComponentInstanceId,
    tag: String,
    endpoint: Destination,
    binding: UpstreamBinding,
}
pub(super) fn transport(services: &TcpRuntimeServices, node: ServiceNode) -> UpstreamTransport {
    let binding = UpstreamBinding {
        prefix: vec![node.slot.role_id()],
        dial: node.dial,
        network_generation: NetworkGeneration(services.upstream().network_generation()),
    };
    UpstreamTransport::new(
        binding.clone(),
        Arc::new(Opener {
            source: services.execution().current_source,
            donor: node.slot.instance_id(),
            tag: node.tag.expect("configured actual Service donor"),
            endpoint: node.endpoint.expect("active Service donor endpoint"),
            binding,
        }),
    )
}
impl UpstreamOpener for Opener {
    fn borrow_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::loan::StreamLoan, ChannelError>,
    > {
        let binding = binding.clone();
        Box::pin(async move {
            if binding != self.binding || destination != self.endpoint || call.session.is_some() {
                return Err(invalid());
            }
            let source = self.source.upgrade().ok_or_else(invalid)?;
            let services = source
                .runtime_services()
                .map_err(|error| invalid().with_source(error))?;
            let node = services
                .service_node(&self.tag)
                .map_err(|error| invalid().with_source(error))?;
            if node.slot.instance_id() != self.donor
                || services.upstream().network_generation() != binding.network_generation.0
            {
                return Err(invalid());
            }
            service::connect_ingress(&services.execution(), &destination, node, &call).await
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { Err(invalid()) })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { Err(invalid()) })
    }
}
fn invalid() -> ChannelError {
    crate::component::instance::invalid(
        Reason::StalePlan,
        "active Source donor or network generation changed",
    )
}
