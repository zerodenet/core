//! Actual network-role Datagram entry with separately metered control TCP.
mod traffic;
use super::{network::NodeOpener, traffic::EstablishmentTraffic};
use crate::{
    component::instance::{invalid, ComponentSlot},
    protocol_registry::TcpExecutionServices,
};
use std::sync::Arc;
pub(crate) use traffic::WireTraffic;
use zero_core::{component::NetworkGeneration, request::Destination};
use zero_platform_tokio::contract::{
    context::OpenContext,
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    stream::BoxStream,
    BoxFuture, CancellationToken, ChannelError, Reason,
};
pub(crate) struct Opened {
    pub(crate) channel: BoxDatagramSession,
    pub(crate) wire: Arc<WireTraffic>,
}
struct Opener {
    control: NodeOpener,
    datagram: NodeOpener,
}
impl UpstreamOpener for Opener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.control.open_stream(binding, destination, context)
    }
    fn open_datagram(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        self.datagram.open_datagram(binding, request, context)
    }
}
pub(crate) async fn open(
    services: &TcpExecutionServices,
    slot: &Arc<ComponentSlot>,
    tag: &str,
    endpoint: Destination,
    request: DatagramRequest,
) -> Result<Opened, ChannelError> {
    let invocation = services.invocation.clone().ok_or_else(|| {
        invalid(
            Reason::InvalidConfig,
            "Datagram requires the existing business identity",
        )
    })?;
    let deadline = invocation
        .deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5));
    let role = slot.datagram(deadline).await?;
    let upstream = services.upstream_observed(tag);
    let traffic = EstablishmentTraffic::new(upstream.observer());
    let wire = WireTraffic::new(upstream.observer());
    let opener = Arc::new(Opener {
        control: NodeOpener::new(
            upstream.clone().with_observer(Some(traffic.clone())),
            endpoint.clone(),
        ),
        datagram: NodeOpener::new(upstream.with_observer(Some(wire.clone())), endpoint),
    });
    let context = OpenContext {
        identity: invocation.identity,
        deadline: invocation.deadline,
        cancel: CancellationToken::new(),
        observer: None,
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![],
                dial: slot.dial.clone(),
                network_generation: NetworkGeneration(opener.control.generation()),
            },
            opener,
        ),
    };
    let session = context.session_identity()?;
    let opened = role.open(request, context).await;
    // Only TCP establishment observations enter this counter. The raw UDP
    // carrier retains the original Outer observer without this control meter.
    if let Some(session) = session {
        services.record_control_traffic(session.0, traffic.finish());
    }
    opened.map(|channel| Opened { channel, wire })
}
