//! Real host-issued business carrier at the physical root of a selected prefix.
use super::*;
use crate::{inventory::service::ServiceNode, protocol_registry::UpstreamConnectServices};
use std::{net::SocketAddr, sync::Mutex};
use zero_platform_tokio::{
    contract::{
        dns::{DnsGeneration, DnsQueryContext, DnsQueryRole, DnsRequest, DnsService},
        network::{CarrierContext, UpstreamOpener},
        stream::BoxStream,
        BoxFuture,
    },
    controlled_carrier::{
        CarrierEndpointObserver, CarrierEndpoints, CarrierGrant, NativeCarrierOpener,
        PreparedCarrier,
    },
};
pub(super) struct Opener {
    pub(super) borrowed: Option<super::borrowed::Prefix>,
    pub(super) health: Arc<crate::component::stream_bridge::relay::health::FirstHopHealth>,
    pub(super) observed: Arc<std::sync::atomic::AtomicBool>,
    root: Destination,
    pub(super) endpoint: Destination,
    pub(super) binding: UpstreamBinding,
    pub(super) business: BusinessCarrierRequest,
    pub(super) network: UpstreamConnectServices,
    pub(super) hops: Vec<prepare::Hop>,
    pub(super) identity: zero_platform_tokio::contract::context::RequestIdentity,
    pub(super) remote: Arc<Mutex<Option<SocketAddr>>>,
    resolved: tokio::sync::Mutex<Option<Resolved>>,
}
struct Resolved {
    service: Option<Arc<dyn DnsService>>,
    generation: Option<DnsGeneration>,
    peers: Vec<SocketAddr>,
    next: usize,
}
struct Evidence(Arc<Mutex<Option<SocketAddr>>>);
impl CarrierEndpointObserver for Evidence {
    fn connected(&self, endpoints: &CarrierEndpoints) {
        *self.0.lock().unwrap() = Some(endpoints.peer());
    }
}
impl Opener {
    #[expect(
        clippy::too_many_arguments,
        reason = "existing physical carrier setup keeps request, scope and native ownership inputs explicit"
    )]
    pub(super) async fn new(
        services: &TcpExecutionServices,
        nodes: Vec<ServiceNode>,
        borrowed: Option<usize>,
        binding: UpstreamBinding,
        endpoint: Destination,
        wire_endpoint: Destination,
        business: BusinessCarrierRequest,
        end: tokio::time::Instant,
    ) -> Result<Self, ChannelError> {
        let first = &nodes[0];
        let health = Arc::new(
            crate::component::stream_bridge::relay::health::FirstHopHealth::new(
                services.engine(),
                &services.snapshot_handle(),
                first.tag.as_deref().unwrap_or(""),
            ),
        );
        let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut hops = Vec::new();
        for (index, node) in nodes.iter().enumerate().skip(borrowed.map_or(0, |i| i + 1)) {
            let (target, tag) = if let Some(next) = nodes.get(index + 1) {
                (
                    next.wire_endpoint.clone().unwrap(),
                    next.tag.as_deref().unwrap_or(""),
                )
            } else {
                (wire_endpoint.clone(), "")
            };
            // Final-hop accounting is supplied separately, without retaining the registry.
            let observer = if tag.is_empty() {
                None
            } else {
                services.outbound_io(tag, zero_api::TrafficPlane::Outer)
            };
            hops.push(prepare::Hop {
                health: (index == 0).then(|| health.clone()),
                observed: observed.clone(),
                role: node.slot.stream(end).await?,
                endpoint: node.endpoint.clone().unwrap(),
                target,
                binding: UpstreamBinding {
                    prefix: binding.prefix[..index].to_vec(),
                    dial: node.dial.clone(),
                    network_generation: binding.network_generation,
                },
                observer,
            });
        }
        let borrowing = match borrowed {
            Some(index) => Some(
                super::borrowed::Prefix::prepare(
                    services,
                    &nodes,
                    index,
                    &binding,
                    &endpoint,
                    &wire_endpoint,
                    end,
                )
                .await?,
            ),
            None => None,
        };
        let first = &nodes[0];
        Ok(Self {
            borrowed: borrowing,
            health,
            observed,
            root: first.endpoint.clone().unwrap(),
            endpoint,
            binding,
            business,
            network: services.upstream_observed(first.tag.as_deref().unwrap_or("")),
            hops,
            identity: services.invocation.as_ref().unwrap().identity.clone(),
            remote: Arc::default(),
            resolved: tokio::sync::Mutex::new(None),
        })
    }
    pub(super) fn observe_result<T>(&self, result: &Result<T, ChannelError>) {
        if !self.observed.load(std::sync::atomic::Ordering::Acquire) {
            self.health.observe(result);
        }
    }
    pub(super) fn remote(&self) -> Option<SocketAddr> {
        *self.remote.lock().unwrap()
    }
    pub(super) fn with_final_observer(
        mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        if let Some(last) = self.hops.last_mut() {
            last.observer = observer;
        } else if let Some(prefix) = self.borrowed.as_mut() {
            prefix.observer = observer;
        }
        self
    }
}
impl Opener {
    fn prepare_root_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        call: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<PreparedCarrier, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if binding != self.binding
                || destination != self.endpoint
                || self.network.network_generation() != binding.network_generation.0
            {
                return Err(invalid(Reason::StalePlan, "relay binding changed"));
            }
            let ticket = self.business.authorize(&destination, &binding, &grant)?;
            let mut resolved = self.resolved.lock().await;
            if resolved.is_none() {
                let (service, generation, peers) = match &self.root.address {
                    Address::Domain(name) => {
                        let service = self.network.native_node_dns();
                        let generation = service.generation();
                        let answer = service
                            .resolve(
                                DnsRequest::new(name, DnsQueryRole::Node),
                                DnsQueryContext {
                                    deadline: call.deadline,
                                    cancel: call.cancel.clone(),
                                    expected_generation: Some(generation),
                                },
                            )
                            .await?;
                        let peers = answer
                            .addresses
                            .into_iter()
                            .map(|ip| {
                                SocketAddr::new(
                                    match ip {
                                        zero_traits::IpAddress::V4(v) => std::net::IpAddr::from(v),
                                        zero_traits::IpAddress::V6(v) => std::net::IpAddr::from(v),
                                    },
                                    self.root.port,
                                )
                            })
                            .collect();
                        (Some(service), Some(generation), peers)
                    }
                    Address::Ipv4(ip) => (
                        None,
                        None,
                        vec![SocketAddr::new((*ip).into(), self.root.port)],
                    ),
                    Address::Ipv6(ip) => (
                        None,
                        None,
                        vec![SocketAddr::new((*ip).into(), self.root.port)],
                    ),
                };
                *resolved = Some(Resolved {
                    service,
                    generation,
                    peers,
                    next: 0,
                });
            }
            let resolved = resolved.as_mut().unwrap();
            let peer = *resolved
                .peers
                .get(resolved.next)
                .ok_or_else(|| invalid(Reason::Io, "relay root candidates exhausted"))?;
            resolved.next += 1;
            let dns = resolved.service.clone().zip(resolved.generation);
            let prepared = NativeCarrierOpener::business_relay(
                ticket,
                self.root.clone(),
                peer,
                dns,
                Arc::new(self.network.egress_control()),
                self.network.observer(),
            )?
            .prepare_controlled_stream(&binding, destination, call.clone(), grant)?;
            prepared
                .with_endpoint_observer(Arc::new(Evidence(self.remote.clone())))
                .with_stream_preparation(prepare::preparation(
                    self.hops.clone(),
                    self.identity.clone(),
                    call,
                    self.network.clone(),
                ))
        })
    }
}
impl UpstreamOpener for Opener {
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        call: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        if self.borrowed.is_some() {
            return self.acquire_borrowed(binding.clone(), destination, call, grant);
        }
        let prepared = self.prepare_root_stream(binding, destination, call.clone(), grant);
        Box::pin(async move {
            let result = async {
                Ok(prepared
                    .await?
                    .open_with_deadline(call.deadline)
                    .await?
                    .into_parts()
                    .0)
            }
            .await;
            result.map_err(|mut error: ChannelError| {
                if zero_platform_tokio::controlled_carrier::original_connect_error(&error).is_some()
                {
                    error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
                }
                error
            })
        })
    }

    fn carrier_dns_generation(&self) -> Option<DnsGeneration> {
        matches!(self.root.address, Address::Domain(_)).then(|| self.network.dns_generation())
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
                "controlled relay requires its original reservation",
            ))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async { Err(invalid(Reason::Unsupported, "relay grants no direct UDP")) })
    }
}
