use super::super::business_carrier::BusinessCarrierRequest;
use super::super::instance::invalid;
use crate::protocol_registry::UpstreamConnectServices;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamOpener},
    stream::BoxStream,
};
use zero_platform_tokio::controlled_carrier::{
    CarrierEndpointObserver, CarrierEndpoints, CarrierGrant, NativeCarrierOpener, PreparedCarrier,
};

mod candidates;
mod dns;
#[cfg(any(target_os = "linux", windows))]
mod process;
mod service_dns;
mod service_selection;
use dns::Resolved;

pub(in crate::component) struct Evidence {
    pub(super) remote: SocketAddr,
    pub(super) local: Option<SocketAddr>,
}
pub(in crate::component) struct NodeOpener {
    services: UpstreamConnectServices,
    plan: Option<std::sync::Weak<crate::component::catalog::ComponentPlan>>,
    endpoint: Destination,
    generation: u64,
    evidence: Arc<Mutex<Option<Evidence>>>,
    business: Option<BusinessCarrierRequest>,
    resolved: tokio::sync::Mutex<Option<Resolved>>,
    service_call: Option<CarrierContext>,
    service_candidates: tokio::sync::Mutex<
        Option<(
            zero_platform_tokio::controlled_carrier::CarrierCandidates,
            usize,
        )>,
    >,
    pending: Option<(UpstreamBinding, PendingCarrierObserver)>,
    carrier_owner: Option<zero_core::component::ComponentInstanceId>,
}
impl NodeOpener {
    pub(in crate::component) fn new(
        services: UpstreamConnectServices,
        endpoint: Destination,
    ) -> Self {
        let generation = services.network_generation();
        Self {
            services,
            plan: None,
            endpoint,
            generation,
            evidence: Arc::new(Mutex::new(None)),
            business: None,
            resolved: tokio::sync::Mutex::new(None),
            service_call: None,
            service_candidates: tokio::sync::Mutex::new(None),
            pending: None,
            carrier_owner: None,
        }
    }
    pub(in crate::component) fn with_plan(
        mut self,
        plan: Option<std::sync::Weak<crate::component::catalog::ComponentPlan>>,
    ) -> Self {
        self.plan = plan;
        self
    }
    pub(in crate::component) fn with_service_call(mut self, call: CarrierContext) -> Self {
        // An actual invocation's cancellation/deadline, never the per-attempt
        // carrier token. Business preparation keeps its existing sealed issuer.
        if call.session.is_none() {
            self.service_call = Some(call);
        }
        self
    }
    pub(in crate::component) fn with_business(
        mut self,
        business: Option<BusinessCarrierRequest>,
    ) -> Self {
        self.business = business;
        self
    }
    pub(in crate::component) fn with_pending(
        mut self,
        binding: UpstreamBinding,
        waiting: Option<PendingCarrierObserver>,
    ) -> Self {
        self.pending = waiting.map(|waiting| (binding, waiting));
        self
    }
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
    pub(in crate::component) fn take_evidence(&self) -> Option<Evidence> {
        self.evidence.lock().expect("node evidence poisoned").take()
    }
}
struct ConnectedEvidence(Arc<Mutex<Option<Evidence>>>);
impl CarrierEndpointObserver for ConnectedEvidence {
    fn connected(&self, endpoints: &CarrierEndpoints) {
        *self.0.lock().expect("node evidence poisoned") = Some(Evidence {
            remote: endpoints.peer(),
            local: endpoints.local(),
        });
    }
}
impl UpstreamOpener for NodeOpener {
    #[cfg(any(target_os = "linux", windows))]
    fn acquire_process_endpoint_stream(
        &self,
        binding: &UpstreamBinding,
        request: zero_platform_tokio::contract::network::ProcessEndpointStreamRequest,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        self.open_process_endpoint_stream(binding.clone(), request)
    }
    fn select_background_carrier(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::network::UpstreamTransport, ChannelError>,
    > {
        self.reselect_service(binding, call, None)
    }
    fn select_background_carrier_observed(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: PendingCarrierObserver,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::network::UpstreamTransport, ChannelError>,
    > {
        self.reselect_service(binding, call, Some(waiting))
    }
    fn resolve_controlled_carrier(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::CarrierCandidates, ChannelError>,
    > {
        self.resolve_background_candidates(binding.clone(), endpoint, call)
    }
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        if !binding.prefix.is_empty()
            || binding.network_generation.0 != self.generation
            || self.services.network_generation() != self.generation
        {
            return Err(invalid(
                Reason::StalePlan,
                "Packet caller node binding changed",
            ));
        }
        if let Some(business) = &self.business {
            return business.packet_caller(identity, call);
        }
        let (actual, waiting) = self.pending.as_ref().ok_or_else(|| {
            invalid(
                Reason::Unsupported,
                "node lacks its actual independent Packet caller",
            )
        })?;
        let invocation = self.service_call.as_ref().ok_or_else(|| {
            invalid(
                Reason::Unsupported,
                "node lacks its actual Service invocation",
            )
        })?;
        if actual != binding
            || call.session.is_some()
            || invocation.session.is_some()
            || call.deadline != invocation.deadline
            || call.cancel.is_cancelled()
            || invocation.cancel.is_cancelled()
            || !matches!(
                identity,
                zero_platform_tokio::contract::context::RequestIdentity::Service(_)
            )
        {
            return Err(invalid(
                Reason::StalePlan,
                "Service Packet caller binding changed",
            ));
        }
        waiting.packet_caller(identity, call)
    }

    fn resolve_packet_target(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        identity: zero_platform_tokio::contract::context::RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<SocketAddr>, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if binding.network_generation.0 != self.generation
                || self.services.network_generation() != self.generation
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "actual Packet target network changed",
                ));
            }
            self.services
                .resolve_inner_packet_target(destination, identity, call)
                .await
        })
    }

    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        self.pending
            .as_ref()
            .filter(|(actual, _)| actual == binding)?
            .1
            .capacity()
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
        let binding = binding.clone();
        Box::pin(async move {
            if !binding.prefix.is_empty()
                || binding.network_generation.0 != self.generation
                || self.services.network_generation() != self.generation
                || call.session.is_some()
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "related Datagram cannot change the actual control binding",
                ));
            }
            let resolver = zero_platform_tokio::controlled_carrier::ResolvingCarrierOpener::new(
                parent.instance(),
                binding.clone(),
                self.endpoint.clone(),
                self.services.native_node_dns(),
                Arc::new(self.services.egress_control()),
                self.services.observer(),
            )?;
            resolver
                .resolve_related_datagram(&binding, parent, peer, call)
                .await
        })
    }

    fn has_tracked_business_caller(&self) -> bool {
        self.business
            .as_ref()
            .is_some_and(|request| request.has_tracked_caller())
    }
    fn has_pending_carrier_observer(&self) -> bool {
        self.pending.is_some()
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let (expected, waiting) = self.pending.as_ref().ok_or_else(|| {
            invalid(
                Reason::Unsupported,
                "node has no actual waiting cleanup owner",
            )
        })?;
        if binding != expected || self.services.network_generation() != self.generation {
            return Err(invalid(Reason::StalePlan, "pending node binding changed"));
        }
        waiting.retain(adoption)
    }
    fn observe_carrier_origin(
        &self,
        origin: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> bool {
        let root = origin.native_origin();
        let control = root.related_control();
        if control
            .as_ref()
            .is_some_and(|control| !control.owner().same_owner(root.owner()))
        {
            return false;
        }
        let authority = control.as_ref().unwrap_or(root);
        let Some((endpoint, binding)) = authority.selected_binding() else {
            return false;
        };
        if endpoint != self.endpoint
            || !binding.prefix.is_empty()
            || binding.network_generation.0 != self.generation
            || self.services.network_generation() != self.generation
            || root.is_retired()
        {
            return false;
        }
        let Some(endpoints) = root.endpoints() else {
            return false;
        };
        ConnectedEvidence(self.evidence.clone()).connected(&endpoints);
        true
    }
    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        if self.carrier_owner.is_some() {
            // The same shared background pool has a Node epoch even for a
            // literal root; nested borrowers must not retire their own setup
            // because prefix metadata and root metadata disagree.
            return Some(self.services.native_node_dns().generation());
        }
        matches!(self.endpoint.address, zero_core::Address::Domain(_))
            .then(|| self.services.dns_generation())
    }
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        let preparing = self.prepare_node_stream(binding, destination, context.clone(), grant);
        Box::pin(async move {
            Ok(preparing
                .await?
                .open_with_deadline(context.deadline)
                .await?
                .into_parts()
                .0)
        })
    }
    fn prepare_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: CarrierGrant,
    ) -> Result<PreparedCarrier, ChannelError> {
        if !binding.prefix.is_empty()
            || binding.network_generation.0 != self.generation
            || self.services.network_generation() != self.generation
            || destination != self.endpoint
        {
            return Err(invalid(
                Reason::StalePlan,
                "controlled carrier cannot bypass selected prefix, endpoint or network generation",
            ));
        }
        let native = if context.session.is_some() {
            let business = self.business.as_ref().ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "controlled business carrier requires the actual admitted ingress demand",
                )
            })?;
            NativeCarrierOpener::business(
                business.authorize(&destination, binding, &grant)?,
                Arc::new(self.services.egress_control()),
                self.services.observer(),
            )?
        } else {
            let ip = match destination.address {
                zero_core::Address::Ipv4(ip) => std::net::IpAddr::from(ip),
                zero_core::Address::Ipv6(ip) => std::net::IpAddr::from(ip),
                zero_core::Address::Domain(_) => {
                    return Err(invalid(
                        Reason::Unsupported,
                        "background roots require an actual host-owned DNS provider",
                    ));
                }
            };
            NativeCarrierOpener::new(
                grant.instance(),
                binding.clone(),
                SocketAddr::new(ip, destination.port),
                Arc::new(self.services.egress_control()),
                self.services.observer(),
            )?
        };
        native
            .prepare_controlled_stream(binding, destination, context, grant)
            .map(|prepared| {
                prepared.with_endpoint_observer(Arc::new(ConnectedEvidence(self.evidence.clone())))
            })
    }

    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if !binding.prefix.is_empty()
                || binding.network_generation.0 != self.generation
                || self.services.network_generation() != self.generation
                || destination != self.endpoint
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "node opener cannot bypass a prefix, endpoint or network generation",
                ));
            }
            if binding.dial != Default::default() {
                return Err(invalid(
                    Reason::InvalidConfig,
                    "non-default dial constraints currently require a direct outbound",
                ));
            }
            let host = match &destination.address {
                zero_core::Address::Domain(host) => host.clone(),
                zero_core::Address::Ipv4(ip) => std::net::Ipv4Addr::from(*ip).to_string(),
                zero_core::Address::Ipv6(ip) => std::net::Ipv6Addr::from(*ip).to_string(),
            };
            // The existing node resolver/bootstrap policy is distinct from
            // business destination DNS. No system-DNS/direct fallback is added.
            let socket = self
                .services
                .connect_upstream_owned(host, destination.port)
                .await
                .map_err(|source| {
                    let mut error = ChannelError::new(Stage::Dial, Reason::Io, Impact::Session)
                        .with_source(source);
                    error.delivery = DeliveryState::NotSubmitted;
                    error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
                    error
                })?;
            if self.services.network_generation() != self.generation {
                return Err(invalid(
                    Reason::StalePlan,
                    "network changed during node establishment",
                ));
            }
            *self.evidence.lock().expect("node evidence poisoned") = Some(Evidence {
                remote: socket.peer_addr().map_err(|source| {
                    ChannelError::new(Stage::Dial, Reason::Io, Impact::Session).with_source(source)
                })?,
                local: socket.local_addr().ok(),
            });
            Ok(Box::pin(socket) as BoxStream)
        })
    }
    fn open_datagram(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        #[cfg(feature = "udp-runtime")]
        {
            let binding = binding.clone();
            Box::pin(async move {
                self.services
                    .open_node_datagram(&binding, request, context)
                    .await
            })
        }
        #[cfg(not(feature = "udp-runtime"))]
        {
            let _ = (binding, request, context);
            Box::pin(async { Err(invalid(Reason::Unsupported, "UDP runtime is disabled")) })
        }
    }
}
