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
    fn select_background_carrier_observed(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: zero_platform_tokio::contract::network::PendingCarrierObserver,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.control.reselect_service(binding, call, Some(waiting))
    }

    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        self.control.packet_caller(binding, identity, call)
    }

    fn resolve_service_datagram_candidates(
        &self,
        binding: &UpstreamBinding,
        issuer: zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTargetIssuer,
    ) -> BoxFuture<
        '_,
        Result<
            zero_platform_tokio::controlled_carrier::loan::BorrowedDatagramCandidates,
            ChannelError,
        >,
    > {
        self.control
            .resolve_service_datagram_candidates(binding, issuer)
    }
    fn resolve_packet_target(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        identity: zero_platform_tokio::contract::context::RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<std::net::SocketAddr>, ChannelError>> {
        self.control
            .resolve_packet_target(binding, destination, identity, call)
    }

    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        self.control.pending_carrier_capacity(binding)
    }

    fn protocol_original_observer(
        &self,
    ) -> Option<zero_platform_tokio::contract::network::PendingCarrierObserver> {
        self.control.protocol_original_observer()
    }
    fn has_pending_carrier_observer(&self) -> bool {
        self.control.has_pending_carrier_observer()
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.control.retain_pending_carrier(binding, adoption)
    }

    fn acquire_process_endpoint_datagram(
        &self,
        binding: &UpstreamBinding,
        request: zero_platform_tokio::contract::network::ProcessEndpointDatagramRequest,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierDatagram, ChannelError>>
    {
        self.datagram
            .acquire_process_endpoint_datagram(binding, request)
    }
    fn resolve_related_datagram(
        &self,
        binding: &UpstreamBinding,
        parent: zero_platform_tokio::controlled_carrier::CarrierAdoption,
        peer: Destination,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::CarrierCandidates, ChannelError>,
    > {
        // UDP retains the actual wire observer, separate from TCP establishment.
        self.datagram
            .resolve_related_datagram(binding, parent, peer, call)
    }

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
/// One existing Packet stack session, without inventing a native node endpoint.
#[cfg(feature = "managed-stream-runtime")]
pub(crate) async fn open_packet_flow(
    services: &TcpExecutionServices,
    slot: &Arc<ComponentSlot>,
    tag: &str,
    request: DatagramRequest,
) -> Result<Opened, ChannelError> {
    use super::packet_network::{PacketObserver, PacketOpener};
    let invocation = services.invocation.as_ref().ok_or_else(|| {
        invalid(
            Reason::InvalidConfig,
            "Packet Datagram requires its actual admitted invocation",
        )
    })?;
    let plan = services
        .components
        .as_ref()
        .map_err(|error| crate::component::instance::retained(error.clone()))?;
    if !plan.has_device_carrier_provider(slot.role_id()) {
        return Err(invalid(
            Reason::StalePlan,
            "Packet Datagram source is not actually live",
        ));
    }
    let network = services.upstream_observed(tag);
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: slot.dial.clone(),
        network_generation: NetworkGeneration(network.network_generation()),
    };
    let role = slot
        .datagram(
            invocation
                .deadline
                .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5)),
        )
        .await?;
    // Legacy Packet Flow counts payload at this boundary. Actual IP/UDP Inner
    // bytes remain with the same stack's existing observer, Outer with Source.
    let wire = WireTraffic::new(None);
    let channel = role
        .open(
            request,
            OpenContext {
                identity: invocation.identity.clone(),
                deadline: invocation.deadline,
                cancel: invocation.cancellation(),
                observer: Some(Arc::new(PacketObserver {
                    engine: services.engine().clone(),
                    tag: tag.into(),
                    session: invocation.identity.session(),
                    traffic: EstablishmentTraffic::new(None),
                    inner: services.outbound_io(tag, zero_api::TrafficPlane::Inner),
                })),
                upstream: UpstreamTransport::new(
                    binding.clone(),
                    Arc::new(PacketOpener {
                        services: network,
                        binding,
                        invocation: invocation.clone(),
                        engine: services.engine().clone(),
                        slot: Arc::downgrade(slot),
                    }),
                ),
            },
        )
        .await?;
    Ok(Opened {
        channel,
        wire,
        remote: None,
    })
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
    let role = slot.datagram(deadline).await.map_err(|mut failure| {
        failure.component.get_or_insert(slot.id);
        failure.session = failure.session.or(invocation.identity.session());
        failure
    })?;
    let upstream = services.upstream_observed(tag);
    let traffic = EstablishmentTraffic::new(upstream.observer());
    let wire = WireTraffic::new(upstream.observer());
    let business = super::super::business_carrier::BusinessCarrierRequest::new(
        services.engine(),
        slot,
        &invocation,
    )?;
    // The real association may serve later packets from other business
    // sessions. Its physical preparation and returned logical control share
    // this one lifetime, rather than the first packet's session cancellation.
    let pending = services
        .pending_datagram
        .as_ref()
        .and_then(std::sync::Weak::upgrade);
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: slot.dial.clone(),
        network_generation: NetworkGeneration(upstream.network_generation()),
    };
    let waiting = match &pending {
        Some(pending) => pending.observer(binding.clone())?,
        None => None,
    };
    let association_cancel = pending
        .map(|pending| pending.cancellation())
        .unwrap_or_else(|| {
            if meter_control {
                CancellationToken::new()
            } else {
                invocation.cancellation()
            }
        });
    let association_call = CarrierContext {
        session: None,
        deadline: invocation.deadline,
        cancel: association_cancel.clone(),
    };
    let opener = Arc::new(Opener {
        control: NodeOpener::new(
            upstream.clone().with_observer(Some(traffic.clone())),
            endpoint.clone(),
        )
        .with_plan(services.components.as_ref().ok().map(Arc::downgrade))
        .with_business(business)
        .with_service_call(association_call.clone())
        .with_pending(binding.clone(), waiting),
        datagram: NodeOpener::new(upstream.with_observer(Some(wire.clone())), endpoint)
            .with_plan(services.components.as_ref().ok().map(Arc::downgrade))
            .with_service_call(association_call),
    });
    let context = OpenContext {
        identity: invocation.identity.clone(),
        deadline: invocation.deadline,
        cancel: association_cancel,
        observer: Some(wire.clone()),
        upstream: UpstreamTransport::new(binding, opener.clone()),
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

pub(crate) mod packet_path;
