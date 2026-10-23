use super::super::business_carrier::BusinessCarrierRequest;
use super::super::instance::invalid;
use crate::protocol_registry::UpstreamConnectServices;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener},
    stream::BoxStream,
    BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
};
use zero_platform_tokio::controlled_carrier::{
    CarrierEndpointObserver, CarrierEndpoints, CarrierGrant, NativeCarrierOpener, PreparedCarrier,
};

pub(in crate::component) struct Evidence {
    pub(super) remote: SocketAddr,
    pub(super) local: Option<SocketAddr>,
}
pub(in crate::component) struct NodeOpener {
    services: UpstreamConnectServices,
    endpoint: Destination,
    generation: u64,
    evidence: Arc<Mutex<Option<Evidence>>>,
    business: Option<BusinessCarrierRequest>,
}
impl NodeOpener {
    pub(in crate::component) fn new(
        services: UpstreamConnectServices,
        endpoint: Destination,
    ) -> Self {
        let generation = services.network_generation();
        Self {
            services,
            endpoint,
            generation,
            evidence: Arc::new(Mutex::new(None)),
            business: None,
        }
    }
    pub(in crate::component) fn with_business(
        mut self,
        business: Option<BusinessCarrierRequest>,
    ) -> Self {
        self.business = business;
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
        let business = self.business.as_ref().ok_or_else(|| {
            invalid(
                Reason::Unsupported,
                "controlled business carrier requires the actual admitted ingress demand",
            )
        })?;
        let ticket = business.authorize(&destination, binding, &grant)?;
        let native = NativeCarrierOpener::business(
            ticket,
            Arc::new(self.services.egress_control()),
            self.services.observer(),
        )?;
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
