//! One actual source main opens a selected L3 role without a business session.
use super::model::InboundRouteRuntimeFactory;
use crate::{component::instance::invalid, runtime::inbound_owners::InboundTaskAccess};
use std::{net::SocketAddr, sync::Arc};
use zero_core::{component::RoleInstanceId, packet::PacketFacts, request::Destination};
use zero_platform_tokio::contract::{
    BoxFuture, CancellationToken, ChannelError, Reason,
    context::{ConnectFact, ConnectObserver, OpenContext, PacketIoFact, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    packet::{BoxPacketSession, PacketRequest},
    scope::TaskJoinWitness,
    stream::BoxStream,
};
struct SourceOpener {
    network: crate::protocol_registry::UpstreamConnectServices,
    binding: UpstreamBinding,
    facts: Arc<PacketFacts>,
    main: TaskJoinWitness,
    stop: CancellationToken,
    admission: zero_runtime::component::ServiceAdmission,
}
impl UpstreamOpener for SourceOpener {
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<TaskJoinWitness, ChannelError> {
        if binding != &self.binding
            || self.network.network_generation() != binding.network_generation.0
            || call.session.is_some()
            || call.deadline.is_some()
            || call.cancel.is_cancelled()
            || self.stop.is_cancelled()
            || self.main.is_joined()
            || !self.main.scope_identity().resource_open()
            || !matches!(identity, RequestIdentity::Packet(facts) if Arc::ptr_eq(facts, &self.facts))
        {
            return Err(invalid(
                Reason::StalePlan,
                "Packet source differs from its actual main and facts",
            ));
        }
        self.admission.check_current()?;
        Ok(self.main.clone())
    }
    fn resolve_packet_target(
        &self,
        binding: &UpstreamBinding,
        target: Destination,
        identity: RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<SocketAddr>, ChannelError>> {
        let admitted = self.packet_caller(binding, &identity, &call);
        Box::pin(async move {
            admitted?;
            self.network
                .resolve_inner_packet_target(target, identity, call)
                .await
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "Packet source grants no native Stream",
            ))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "Packet source grants no native Datagram",
            ))
        })
    }
}
struct SourceObserver(Option<Arc<dyn zero_traits::IoObserver>>);
impl ConnectObserver for SourceObserver {
    fn record(&self, _: Option<zero_core::component::SessionId>, _: ConnectFact) {}
    fn packet_flow_io(&self) -> Option<Arc<dyn zero_traits::IoObserver>> {
        self.0.clone()
    }
    fn packet_io(&self, _: &zero_core::packet::ReturnAssociation, fact: PacketIoFact) {
        let Some(observer) = &self.0 else {
            return;
        };
        match fact {
            PacketIoFact::Sent { bytes } => observer.sent(bytes),
            PacketIoFact::Received { bytes } => observer.received(bytes),
            PacketIoFact::Dropped { reason, .. } => observer.dropped_reason(reason),
            PacketIoFact::Error => observer.error(),
            PacketIoFact::ReceiveCoverageLost => observer.receive_coverage_lost(),
        }
    }
}
impl InboundRouteRuntimeFactory {
    pub(in crate::runtime) async fn open_packet_role(
        &self,
        selected: RoleInstanceId,
        facts: Arc<PacketFacts>,
        request: PacketRequest,
        source: &InboundTaskAccess,
        cancel: CancellationToken,
        inner: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<BoxPacketSession, ChannelError> {
        let actual = source.source_instance();
        let inbound = self
            .for_connection(None)
            .inbound_instance()
            .map_err(|error| {
                invalid(Reason::StalePlan, "source inbound identity is unavailable")
                    .with_source(error)
            })?;
        if facts.inbound != inbound
            || facts.inbound.component != actual.component
            || facts.inbound.generation != actual.generation
            || facts.endpoint.0 != actual.component.0
            || request.endpoint != facts.endpoint
            || cancel.is_cancelled()
        {
            return Err(invalid(
                Reason::StalePlan,
                "Packet role differs from its original source endpoint",
            ));
        }
        let main = source.source_main_witness().await?;
        let services = self.shared.tcp_services().with_current_snapshot();
        services.ensure_configuration_committed().map_err(|error| {
            invalid(Reason::StalePlan, "Packet candidate is not committed").with_source(error)
        })?;
        let execution = services.execution();
        let plan = execution
            .components
            .as_ref()
            .map_err(|source| crate::component::instance::retained(source.clone()))?;
        let slot = plan
            .role_slot(selected)
            .ok_or_else(|| invalid(Reason::StalePlan, "selected Packet generation changed"))?;
        if !plan.has_packet_provider(selected) {
            return Err(invalid(
                Reason::StalePlan,
                "selected Packet has no actual native source",
            ));
        }
        let tag = plan
            .role_tag(selected)
            .ok_or_else(|| invalid(Reason::StalePlan, "selected Packet owner has no tag"))?;
        let network = execution.upstream_observed(tag);
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: slot.dial.clone(),
            network_generation: zero_core::component::NetworkGeneration(
                network.network_generation(),
            ),
        };
        let opener = Arc::new(SourceOpener {
            network,
            binding: binding.clone(),
            facts: facts.clone(),
            main,
            stop: source.cancellation(),
            admission: slot
                .service_admission()
                .ok_or_else(|| invalid(Reason::StalePlan, "Packet owner has no admission"))?,
        });
        let identity = RequestIdentity::Packet(facts);
        opener.packet_caller(
            &binding,
            &identity,
            &CarrierContext {
                session: None,
                deadline: None,
                cancel: cancel.clone(),
            },
        )?;
        let role = slot
            .packet(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
            .await?;
        role.open(
            request,
            OpenContext {
                identity,
                deadline: None,
                cancel,
                observer: Some(Arc::new(SourceObserver(inner))),
                upstream: UpstreamTransport::new(binding, opener),
            },
        )
        .await
    }
}
