use super::super::instance::invalid;
use crate::protocol_registry::UpstreamConnectServices;
use std::{net::SocketAddr, sync::Mutex};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener},
    stream::BoxStream,
    BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub(super) struct Evidence {
    pub(super) remote: SocketAddr,
    pub(super) local: Option<SocketAddr>,
}
pub(super) struct NodeOpener {
    services: UpstreamConnectServices,
    endpoint: Destination,
    generation: u64,
    evidence: Mutex<Option<Evidence>>,
}
impl NodeOpener {
    pub(super) fn new(services: UpstreamConnectServices, endpoint: Destination) -> Self {
        let generation = services.network_generation();
        Self {
            services,
            endpoint,
            generation,
            evidence: Mutex::new(None),
        }
    }
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
    pub(super) fn take_evidence(&self) -> Option<Evidence> {
        self.evidence.lock().expect("node evidence poisoned").take()
    }
}
impl UpstreamOpener for NodeOpener {
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
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Internal,
                "node Stream opener has no datagram grant",
            ))
        })
    }
}
