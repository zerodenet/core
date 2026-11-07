//! An actual background-owned pool selects its physical prefix itself.
//! This invocation supplies the selected binding and observes the real lineage.
use super::*;
use zero_platform_tokio::contract::{
    datagram::BoxDatagramSession,
    network::{CarrierContext, UpstreamOpener},
    stream::BoxStream,
    BoxFuture,
};

struct Selected {
    upstream: UpstreamTransport,
    remote: std::sync::Mutex<Option<std::net::SocketAddr>>,
}
impl UpstreamOpener for Selected {
    fn select_background_carrier_observed(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: zero_platform_tokio::contract::network::PendingCarrierObserver,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.upstream
            .select_background_carrier_observed(binding, call, waiting)
    }

    fn has_pending_carrier_observer(&self) -> bool {
        self.upstream.observed_pending_carrier().is_some()
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        if binding != self.upstream.binding() {
            return None;
        }
        self.upstream.pending_carrier_capacity()
    }
    fn packet_driver_custody(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<
        Option<zero_platform_tokio::contract::component::PacketDriverCallRetained>,
        ChannelError,
    > {
        self.packet_caller(binding, identity, call)?;
        self.upstream.packet_driver_custody(identity, call)
    }
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        if binding != self.upstream.binding() {
            return Err(invalid(Reason::StalePlan, "Packet caller binding changed"));
        }
        self.upstream.packet_caller(identity, call)
    }
    fn borrow_service_datagram_candidate(
        &self,
        binding: &UpstreamBinding,
        target: zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTarget,
    ) -> BoxFuture<
        '_,
        Result<
            zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTargetLoan,
            ChannelError,
        >,
    > {
        let valid = binding == self.upstream.binding();
        Box::pin(async move {
            if !valid {
                return Err(ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    Reason::StalePlan,
                    zero_platform_tokio::contract::Impact::Component,
                ));
            }
            self.upstream
                .borrow_service_datagram_candidate(target)
                .await
        })
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
        if binding != self.upstream.binding() || issuer.binding() != binding {
            return Box::pin(async {
                Err(crate::component::instance::invalid(
                    Reason::StalePlan,
                    "actual Service candidate binding changed",
                ))
            });
        }
        self.upstream.resolve_service_datagram_candidates(issuer)
    }
    fn resolve_packet_target(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        identity: zero_platform_tokio::contract::context::RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<std::net::SocketAddr>, ChannelError>> {
        if binding != self.upstream.binding() {
            return Box::pin(async {
                Err(invalid(Reason::StalePlan, "packet target binding changed"))
            });
        }
        self.upstream
            .resolve_packet_target(destination, identity, call)
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if binding != self.upstream.binding() {
            return Err(invalid(Reason::StalePlan, "pooled pending binding changed"));
        }
        self.upstream.retain_pending_carrier(adoption)
    }

    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        self.upstream.carrier_dns_generation()
    }
    fn observe_carrier_origin(
        &self,
        origin: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> bool {
        // The real selected prefix validates its original root endpoint and
        // network. An unvalidated observation cannot supply connection facts.
        if !self.upstream.observe_carrier_origin(origin) {
            return false;
        }
        let Some(endpoints) = origin.native_origin().endpoints() else {
            return false;
        };
        *self.remote.lock().unwrap() = Some(endpoints.peer());
        true
    }
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let valid = binding == self.upstream.binding();
        Box::pin(async move {
            if !valid {
                return Err(invalid(Reason::StalePlan, "pooled relay binding changed"));
            }
            self.upstream.open_stream(endpoint, call).await
        })
    }
    fn open_datagram(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        let valid = binding == self.upstream.binding();
        Box::pin(async move {
            if !valid {
                return Err(invalid(Reason::StalePlan, "pooled relay binding changed"));
            }
            self.upstream.open_datagram(request, call).await
        })
    }
}

pub(super) async fn open(
    services: &TcpExecutionServices,
    prepared: PreparedDatagramRelay,
    session: &Session,
    association: zero_core::component::AssociationId,
) -> Result<Opened, ChannelError> {
    let slot = prepared.target.current_slot(services).map_err(|source| {
        invalid(Reason::StalePlan, "pooled Datagram owner changed").with_source(source)
    })?;
    let invocation = services.invocation.clone().ok_or_else(|| {
        invalid(
            Reason::InvalidConfig,
            "pooled Datagram requires original business admission",
        )
    })?;
    let end = invocation
        .deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(5));
    let role = slot.datagram(end).await?;
    let binding = UpstreamBinding {
        prefix: prepared
            .prefix
            .iter()
            .map(|node| node.slot.role_id())
            .collect(),
        dial: slot.dial.clone(),
        network_generation: NetworkGeneration(services.upstream().network_generation()),
    };
    services
        .components
        .as_ref()
        .map_err(|source| crate::component::instance::retained(source.clone()))?
        .authorize_carrier_prefix(slot.instance_id(), &binding.prefix)?;
    let tag = prepared.target.tag();
    let (server, port) = prepared.target.endpoint().ok_or_else(|| {
        invalid(
            Reason::StalePlan,
            "pooled network relay tail lacks its declared endpoint",
        )
    })?;
    let pending = services
        .pending_datagram
        .as_ref()
        .and_then(std::sync::Weak::upgrade);
    let association_cancel = pending
        .as_ref()
        .map(|pending| pending.cancellation())
        .ok_or_else(|| {
            invalid(
                Reason::InvalidConfig,
                "pooled Datagram lacks its actual association lifetime",
            )
        })?;
    let upstream = super::super::background::BackgroundPath {
        nodes: prepared.prefix,
        binding: binding.clone(),
        endpoint: crate::inventory::service::endpoint(server, port),
        wire_endpoint: Destination::new(Address::Domain(server.into()), port),
        final_tag: tag,
        call: CarrierContext {
            session: None,
            deadline: invocation.deadline,
            cancel: association_cancel.clone(),
        },
    }
    .select_observed(
        services,
        match &pending {
            Some(pending) => pending.observer(binding.clone())?,
            None => None,
        },
    )
    .await?;
    let selected = Arc::new(Selected {
        upstream,
        remote: Default::default(),
    });
    let wire = WireTraffic::new(services.outbound_io(tag, zero_api::TrafficPlane::Outer));
    let channel = role
        .open(
            DatagramRequest {
                association,
                targets: DatagramTargets::Fixed(Destination::new(
                    session.target.clone(),
                    session.port,
                )),
            },
            OpenContext {
                identity: invocation.identity.clone(),
                deadline: invocation.deadline,
                cancel: association_cancel.clone(),
                observer: Some(wire.clone()),
                upstream: UpstreamTransport::new(binding, selected.clone()),
            },
        )
        .await?;
    let remote = *selected.remote.lock().unwrap();
    Ok(Opened {
        channel,
        wire,
        remote,
    })
}
