pub(crate) mod service;
use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Weak};

use zero_core::{Address, Network, ProtocolType, Session};
#[cfg(feature = "raw-ip-runtime")]
use zero_dns::DnsOutboundDatagramFuture;
use zero_dns::{
    DnsConnectContext, DnsOutboundCarrier, DnsOutboundCarrierFuture, DnsOutboundConnectFuture,
    DnsOutboundConnector, DnsSystem,
};
use zero_engine::Engine;
#[cfg(feature = "raw-ip-runtime")]
use zero_engine::ResolvedOutbound;

use crate::inventory::{ProtocolInventory, WeakProtocolInventory};
use crate::protocol_registry::TcpRuntimeServices;
use crate::runtime::principal_rate_limit::PrincipalRateLimitRegistry;
use crate::transport::extract_tcp_stream;

#[derive(Clone)]
pub(super) struct ProxyDnsOutboundConnector {
    engine: Engine,
    resolver: Weak<DnsSystem>,
    protocols: WeakProtocolInventory,
    egress_interface: zero_platform_tokio::EgressInterfaceControl,
    principal_rate_limits: PrincipalRateLimitRegistry,
    retired_datagrams: Weak<crate::component::datagram_retirement::RetiredDatagrams>,
}

impl ProxyDnsOutboundConnector {
    pub(super) fn new(
        engine: Engine,
        resolver: &Arc<DnsSystem>,
        protocols: ProtocolInventory,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
        principal_rate_limits: PrincipalRateLimitRegistry,
        retired_datagrams: Arc<crate::component::datagram_retirement::RetiredDatagrams>,
    ) -> Self {
        Self {
            engine,
            resolver: Arc::downgrade(resolver),
            // Pooled carriers retain network services and their DNS resolver.
            // The resolver's detour callback must not own those same pools.
            protocols: protocols.downgrade(),
            egress_interface,
            principal_rate_limits,
            retired_datagrams: Arc::downgrade(&retired_datagrams),
        }
    }

    fn runtime_services(&self) -> io::Result<TcpRuntimeServices> {
        let resolver = self.resolver.upgrade().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "DNS runtime is shutting down")
        })?;
        let protocols = self.protocols.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "proxy runtime is shutting down",
            )
        })?;
        let retired_datagrams = self.retired_datagrams.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "datagram owner is shutting down",
            )
        })?;
        Ok(TcpRuntimeServices::new(
            self.engine.clone(),
            self.engine.runtime_snapshot(),
            resolver,
            protocols,
            self.egress_interface.clone(),
            self.principal_rate_limits.clone(),
            retired_datagrams,
        ))
    }
}

impl fmt::Debug for ProxyDnsOutboundConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ProxyDnsOutboundConnector").finish()
    }
}

use service::DnsDetourFailure;

impl DnsOutboundConnector for ProxyDnsOutboundConnector {
    fn connect(&self, outbound: String, endpoint: SocketAddr) -> DnsOutboundConnectFuture {
        self.connect_with_context(outbound, endpoint, DnsConnectContext::default())
    }

    fn connect_with_context(
        &self,
        outbound: String,
        endpoint: SocketAddr,
        context: DnsConnectContext,
    ) -> DnsOutboundConnectFuture {
        let carrier = self.connect_carrier_with_context(outbound, endpoint, context);
        Box::pin(async move { carrier.await.map(|carrier| carrier.stream) })
    }

    fn connect_carrier_with_context(
        &self,
        outbound: String,
        endpoint: SocketAddr,
        context: DnsConnectContext,
    ) -> DnsOutboundCarrierFuture {
        let connector = self.clone();
        let call = context.clone();
        let operation = async move {
            let services = connector.runtime_services()?;
            let target_id = services
                .snapshot()
                .plan()
                .target_id(&outbound)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("DNS detour `{outbound}` was not found"),
                    )
                })?;
            let selections = crate::inventory::service::ServiceSelections::capture(
                services.snapshot(),
                target_id,
            );
            let (resolved, _plan) = services
                .engine()
                .resolve_target_id_in_snapshot(services.snapshot(), target_id)
                .ok_or_else(|| {
                    io::Error::other(format!("DNS detour `{outbound}` could not be resolved"))
                })?;
            let protocols = connector.protocols.upgrade().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "proxy runtime is shutting down",
                )
            })?;
            if let Some(plan) =
                service::prepare(&protocols, &services, &resolved).map_err(|failure| {
                    io::Error::other(DnsDetourFailure {
                        outbound: outbound.clone(),
                        stage: failure.stage,
                        source: failure.error,
                    })
                })?
            {
                // A native failure is final here. Falling back to the legacy
                // bridge after any submission could duplicate a DNS request.
                let connected = service::connect(
                    services.execution(),
                    zero_core::request::Destination::new(
                        endpoint_address(endpoint.ip()),
                        endpoint.port(),
                    ),
                    plan,
                    zero_core::request::ServicePurpose::DnsQuery,
                    zero_platform_tokio::contract::network::CarrierContext {
                        session: None,
                        deadline: call.deadline,
                        cancel: call.cancel,
                    },
                )
                .await
                .map_err(io::Error::other)?;
                let binding = selections
                    .and_then(|selections| selections.chosen(&connected.binding.tags()))
                    .map(|selections| {
                        Arc::new(service::NativeDnsBinding {
                            carrier: connected.binding,
                            selections,
                        }) as Arc<dyn zero_dns::DnsCarrierBinding>
                    });
                return Ok(DnsOutboundCarrier {
                    stream: connected.stream,
                    binding,
                });
            }
            let session = Session::new(
                0,
                endpoint_address(endpoint.ip()),
                endpoint.port(),
                Network::Tcp,
                ProtocolType::UNKNOWN,
            );
            let established = crate::runtime::tcp_dispatch::dispatch_tcp_outbound(
                services,
                &session,
                resolved,
                zero_engine::RouteMode::Auto,
                crate::runtime::tcp_dispatch::TcpDispatchIntent::DnsDetour,
            )
            .await
            .map_err(|failure| {
                io::Error::other(DnsDetourFailure {
                    outbound: outbound.clone(),
                    stage: failure.stage,
                    source: failure.error,
                })
            })?;
            extract_tcp_stream(established)
                .map(|result| DnsOutboundCarrier {
                    stream: result.upstream,
                    binding: None,
                })
                .map_err(|source| {
                    io::Error::other(DnsDetourFailure {
                        outbound,
                        stage: "extract_stream",
                        source,
                    })
                })
        };
        Box::pin(async move {
            if context
                .deadline
                .is_some_and(|end| tokio::time::Instant::now() >= end)
            {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DNS detour deadline elapsed",
                ));
            }
            tokio::select! {biased;
                _=context.cancel.cancelled()=>Err(io::Error::new(io::ErrorKind::Interrupted,"DNS detour cancelled")),
                result=zero_platform_tokio::contract::context::with_deadline(context.deadline,operation)=>result.map_err(|source|io::Error::new(io::ErrorKind::TimedOut,source))?,
            }
        })
    }

    #[cfg(feature = "raw-ip-runtime")]
    fn exchange_datagram(
        &self,
        outbound: String,
        endpoint: SocketAddr,
        query: Vec<u8>,
    ) -> DnsOutboundDatagramFuture {
        let connector = self.clone();
        Box::pin(async move {
            let snapshot = connector.engine.runtime_snapshot();
            let target_id = snapshot.plan().target_id(&outbound).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("DNS detour `{outbound}` was not found"),
                )
            })?;
            let (resolved, _) = connector
                .engine
                .resolve_target_id_in_snapshot(&snapshot, target_id)
                .ok_or_else(|| io::Error::other("DNS datagram detour could not be resolved"))?;
            let ResolvedOutbound::Single(leaf) = resolved else {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "DNS datagram detour requires one packet-capable outbound",
                ));
            };
            let protocols = connector.protocols.upgrade().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "proxy runtime is shutting down",
                )
            })?;
            let claimed = protocols
                .claim_outbound_leaf(snapshot.config(), leaf)
                .map_err(io::Error::other)?;
            let operation = claimed.prepare_datagram_exchange().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "DNS detour outbound has no datagram exchange capability",
                )
            })?;
            operation
                .exchange(
                    endpoint,
                    query,
                    connector.egress_interface.generation(),
                    crate::runtime::traffic_io::outbound(
                        &connector.engine,
                        claimed.runtime().tag.as_deref().unwrap_or(&outbound),
                        zero_api::TrafficPlane::Inner,
                    ),
                )
                .await
        })
    }
}

fn endpoint_address(address: IpAddr) -> Address {
    match address {
        IpAddr::V4(address) => Address::Ipv4(address.octets()),
        IpAddr::V6(address) => Address::Ipv6(address.octets()),
    }
}

#[cfg(all(test, feature = "dns"))]
mod tests;
