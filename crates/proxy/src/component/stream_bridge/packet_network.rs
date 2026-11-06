//! Inner Packet target resolution without a fabricated native TCP endpoint.
use super::*;
use crate::protocol_registry::UpstreamConnectServices;
use std::net::SocketAddr;
use zero_platform_tokio::contract::{
    BoxFuture,
    context::{ConnectFact, ConnectObserver, RequestIdentity},
    network::{CarrierContext, UpstreamOpener},
};
pub(super) struct PacketOpener {
    pub(super) services: UpstreamConnectServices,
    pub(super) binding: UpstreamBinding,
}
impl UpstreamOpener for PacketOpener {
    fn resolve_packet_target(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        identity: RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<SocketAddr>, ChannelError>> {
        let valid = binding == &self.binding
            && binding.network_generation.0 == self.services.network_generation();
        Box::pin(async move {
            if !valid {
                return Err(invalid(Reason::StalePlan, "inner Packet binding changed"));
            }
            self.services
                .resolve_inner_packet_target(destination, identity, call)
                .await
        })
    }
}
pub(super) struct PacketObserver {
    pub(super) engine: zero_engine::Engine,
    pub(super) tag: String,
    pub(super) session: Option<zero_core::component::SessionId>,
    pub(super) traffic: Arc<traffic::EstablishmentTraffic>,
}
impl ConnectObserver for PacketObserver {
    fn record(&self, session: Option<zero_core::component::SessionId>, fact: ConnectFact) {
        self.traffic.record(session, fact);
    }
    fn protocol_traffic(
        &self,
        session: Option<zero_core::component::SessionId>,
        read: u64,
        written: u64,
    ) {
        self.traffic.protocol_traffic(session, read, written);
    }
    fn packet_peer_selected(
        &self,
        session: Option<zero_core::component::SessionId>,
        peer: zero_core::component::PeerId,
    ) {
        if let Some(session) = session.filter(|session| Some(*session) == self.session) {
            self.engine
                .bind_session_peer(session.0, &self.tag, true, &peer.0);
        }
    }
}
pub(super) async fn connect(
    services: &TcpExecutionServices,
    session: &Session,
    slot: &Arc<ComponentSlot>,
    prepared: &PreparedTcpCandidate,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let failure = |error| TcpOutboundFailure {
        stage: slot.legacy_connect_stage(),
        error: zero_engine::EngineError::Io(io::Error::other(LegacyError(error))),
        upstream_endpoint: None,
        network: None,
    };
    let plan = services
        .components
        .as_ref()
        .map_err(|source| failure(super::super::instance::retained(source.clone())))?;
    if !plan.has_device_carrier_provider(slot.role_id()) {
        return Err(failure(invalid(
            Reason::StalePlan,
            "Packet carrier lacks its live actual source",
        )));
    }
    let invocation = services.invocation.as_ref().ok_or_else(|| {
        failure(invalid(
            Reason::Internal,
            "Packet stream requires original request identity",
        ))
    })?;
    let tag = prepared.tag.as_ref().ok_or_else(|| {
        failure(invalid(
            Reason::InvalidConfig,
            "Packet stream has no configured owner",
        ))
    })?;
    let network = services.upstream_observed(tag);
    let traffic = traffic::EstablishmentTraffic::new(network.observer());
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: slot.dial.clone(),
        network_generation: NetworkGeneration(network.network_generation()),
    };
    let role = slot
        .stream(
            invocation
                .deadline
                .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5)),
        )
        .await
        .map_err(failure)?;
    let connected = role
        .connect(
            ConnectRequest {
                destination: Destination::new(session.target.clone(), session.port),
            },
            OpenContext {
                identity: invocation.identity.clone(),
                deadline: invocation.deadline,
                cancel: invocation.cancellation(),
                observer: Some(Arc::new(PacketObserver {
                    engine: services.engine().clone(),
                    tag: tag.clone(),
                    session: invocation.identity.session(),
                    traffic: traffic.clone(),
                })),
                upstream: UpstreamTransport::new(
                    binding.clone(),
                    Arc::new(PacketOpener {
                        services: network,
                        binding,
                    }),
                ),
            },
        )
        .await
        .map_err(failure)?;
    services.record_control_traffic(session.id, traffic.finish());
    Ok(EstablishedTcpOutbound::relay(
        tag.clone(),
        None,
        vec![],
        zero_platform_tokio::TcpRelayStream::from_client(LegacyStream::with_endpoints(
            connected.io,
            None,
            None,
        )),
    ))
}
