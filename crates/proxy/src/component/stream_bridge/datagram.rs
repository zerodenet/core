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
    pub(crate) remote: Option<std::net::SocketAddr>,
}
struct Opener {
    control: NodeOpener,
    datagram: NodeOpener,
}
impl UpstreamOpener for Opener {
    fn observe_carrier_origin(
        &self,
        origin: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> bool {
        self.control.observe_carrier_origin(origin)
    }

    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        self.control.carrier_dns_generation()
    }
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: zero_platform_tokio::controlled_carrier::CarrierGrant,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        self.control
            .acquire_controlled_stream(binding, destination, context, grant)
    }
    fn prepare_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: zero_platform_tokio::controlled_carrier::CarrierGrant,
    ) -> Result<zero_platform_tokio::controlled_carrier::PreparedCarrier, ChannelError> {
        self.control
            .prepare_controlled_stream(binding, destination, context, grant)
    }
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
#[cfg(feature = "upstream-association-runtime")]
pub(crate) async fn open(
    services: &TcpExecutionServices,
    slot: &Arc<ComponentSlot>,
    tag: &str,
    endpoint: Destination,
    request: DatagramRequest,
) -> Result<Opened, ChannelError> {
    open_with_accounting(services, slot, tag, endpoint, request, true).await
}
#[cfg(feature = "managed-stream-runtime")]
pub(crate) async fn open_flow(
    services: &TcpExecutionServices,
    slot: &Arc<ComponentSlot>,
    tag: &str,
    endpoint: Destination,
    request: DatagramRequest,
) -> Result<Opened, ChannelError> {
    open_with_accounting(services, slot, tag, endpoint, request, false).await
}
async fn open_with_accounting(
    services: &TcpExecutionServices,
    slot: &Arc<ComponentSlot>,
    tag: &str,
    endpoint: Destination,
    request: DatagramRequest,
    meter_control: bool,
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
    let business = super::super::business_carrier::BusinessCarrierRequest::new(
        services.engine(),
        slot,
        &invocation,
    )?;
    let opener = Arc::new(Opener {
        control: NodeOpener::new(
            upstream.clone().with_observer(Some(traffic.clone())),
            endpoint.clone(),
        )
        .with_business(business),
        datagram: NodeOpener::new(upstream.with_observer(Some(wire.clone())), endpoint),
    });
    let context = OpenContext {
        identity: invocation.identity.clone(),
        deadline: invocation.deadline,
        cancel: if meter_control {
            CancellationToken::new()
        } else {
            invocation.cancellation()
        },
        observer: Some(wire.clone()),
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![],
                dial: slot.dial.clone(),
                network_generation: NetworkGeneration(opener.control.generation()),
            },
            opener.clone(),
        ),
    };
    let session = context.session_identity()?;
    let opened = role.open(request, context).await;
    // Only TCP establishment observations enter this counter. The raw UDP
    // carrier retains the original Outer observer without this control meter.
    let control_traffic = traffic.finish();
    if let Some(session) = session.filter(|_| meter_control) {
        services.record_control_traffic(session.0, control_traffic);
    }
    opened.map(|channel| Opened {
        channel,
        wire,
        remote: opener
            .control
            .take_evidence()
            .map(|evidence| evidence.remote),
    })
}
