//! Node resolution retains original domain authority and real egress generation.
use super::{error, CarrierDatagram, CarrierGrant, NativeCarrierOpener, PreparedCarrier};
use crate::contract::dns::{DnsGeneration, DnsQueryContext, DnsQueryRole, DnsRequest, DnsService};
use crate::{
    contract::{
        datagram::{BoxDatagramSession, DatagramRequest},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        stream::BoxStream,
        BoxFuture, ChannelError, DeliveryState, Reason, Stage,
    },
    EgressInterfaceControl,
};
use std::{net::SocketAddr, sync::Arc};
use zero_core::{component::ComponentInstanceId, request::Destination, Address};

mod candidates;
pub use candidates::CarrierCandidates;
pub(super) use candidates::Source;
pub(super) fn destination(peer: SocketAddr) -> Destination {
    Destination::new(
        match peer.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        peer.port(),
    )
}
/// Cold, host-supplied endpoint resolver. No sockets or workers are created by
/// construction or by a public ServicePurpose label.
pub struct ResolvingCarrierOpener {
    source: Source,
    resolver: Arc<dyn DnsService>,
}
impl ResolvingCarrierOpener {
    pub fn new(
        grantee: ComponentInstanceId,
        binding: UpstreamBinding,
        endpoint: Destination,
        resolver: Arc<dyn DnsService>,
        egress: Arc<EgressInterfaceControl>,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<Self, ChannelError> {
        if endpoint.port == 0
            || !binding.prefix.is_empty()
            || binding.network_generation.0 != egress.generation()
        {
            return Err(error(
                grantee,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(Self {
            source: Source {
                grantee,
                binding,
                endpoint,
                egress,
                observer,
            },
            resolver,
        })
    }
}
impl UpstreamOpener for ResolvingCarrierOpener {
    fn carrier_dns_generation(&self) -> Option<DnsGeneration> {
        Some(self.resolver.generation())
    }
    fn resolve_controlled_carrier(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<CarrierCandidates, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            self.source.check(&binding, &endpoint, &context)?;
            let generation = self.resolver.generation();
            let peers = match &endpoint.address {
                Address::Domain(host) => {
                    let mut request = DnsRequest::new(host, DnsQueryRole::Node);
                    request.family = binding.dial.effective_family().map_err(|source| {
                        error(
                            self.source.grantee,
                            Stage::Prepare,
                            Reason::InvalidConfig,
                            DeliveryState::NotSubmitted,
                        )
                        .with_source(source)
                    })?;
                    let resolving = self.resolver.resolve(
                        request,
                        DnsQueryContext {
                            deadline: context.deadline,
                            cancel: context.cancel.clone(),
                            expected_generation: Some(generation),
                        },
                    );
                    let answer = tokio::select! { biased;
                        _ = context.cancel.cancelled() => return Err(error(self.source.grantee, Stage::Dial, Reason::Cancelled, DeliveryState::NotSubmitted)),
                        result = crate::contract::context::with_deadline(context.deadline, resolving) => result.map_err(|source| error(self.source.grantee, Stage::Dial, Reason::Timeout, DeliveryState::NotSubmitted).with_source(source))??,
                    };
                    if answer.generation != generation || self.resolver.generation() != generation {
                        return Err(error(
                            self.source.grantee,
                            Stage::Dial,
                            Reason::StalePlan,
                            DeliveryState::NotSubmitted,
                        ));
                    }
                    answer
                        .addresses
                        .into_iter()
                        .map(|ip| {
                            SocketAddr::new(
                                match ip {
                                    zero_traits::IpAddress::V4(ip) => {
                                        std::net::Ipv4Addr::from(ip).into()
                                    }
                                    zero_traits::IpAddress::V6(ip) => {
                                        std::net::Ipv6Addr::from(ip).into()
                                    }
                                },
                                endpoint.port,
                            )
                        })
                        .collect()
                }
                _ => vec![match endpoint.address {
                    Address::Ipv4(ip) => {
                        SocketAddr::new(std::net::Ipv4Addr::from(ip).into(), endpoint.port)
                    }
                    Address::Ipv6(ip) => {
                        SocketAddr::new(std::net::Ipv6Addr::from(ip).into(), endpoint.port)
                    }
                    _ => unreachable!(),
                }],
            };
            self.source.check(&binding, &endpoint, &context)?;
            if peers.is_empty()
                || peers
                    .iter()
                    .any(|peer| peer.port() != endpoint.port || peer.ip().is_unspecified())
            {
                return Err(error(
                    self.source.grantee,
                    Stage::Dial,
                    Reason::Io,
                    DeliveryState::NotSubmitted,
                ));
            }
            let mut candidates = Source {
                grantee: self.source.grantee,
                binding,
                endpoint,
                egress: self.source.egress.clone(),
                observer: self.source.observer.clone(),
            }
            .candidates(peers, context);
            candidates.resolver = Some((self.resolver.clone(), generation));
            Ok(candidates)
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            Err(error(
                self.source.grantee,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
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
            Err(error(
                self.source.grantee,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ))
        })
    }
}
