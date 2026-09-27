use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Weak};

use zero_core::{Address, Network, ProtocolType, Session};
#[cfg(feature = "raw-ip-runtime")]
use zero_dns::DnsOutboundDatagramFuture;
use zero_dns::{DnsOutboundConnectFuture, DnsOutboundConnector, DnsSystem};
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
}

impl ProxyDnsOutboundConnector {
    pub(super) fn new(
        engine: Engine,
        resolver: &Arc<DnsSystem>,
        protocols: ProtocolInventory,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
        principal_rate_limits: PrincipalRateLimitRegistry,
    ) -> Self {
        Self {
            engine,
            resolver: Arc::downgrade(resolver),
            // Pooled carriers retain network services and their DNS resolver.
            // The resolver's detour callback must not own those same pools.
            protocols: protocols.downgrade(),
            egress_interface,
            principal_rate_limits,
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
        Ok(TcpRuntimeServices::new(
            self.engine.clone(),
            self.engine.runtime_snapshot(),
            resolver,
            protocols,
            self.egress_interface.clone(),
            self.principal_rate_limits.clone(),
        ))
    }
}

impl fmt::Debug for ProxyDnsOutboundConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ProxyDnsOutboundConnector").finish()
    }
}

impl DnsOutboundConnector for ProxyDnsOutboundConnector {
    fn connect(&self, outbound: String, endpoint: SocketAddr) -> DnsOutboundConnectFuture {
        let connector = self.clone();
        Box::pin(async move {
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
            let (resolved, _plan) = services
                .engine()
                .resolve_target_id_in_snapshot(services.snapshot(), target_id)
                .ok_or_else(|| {
                    io::Error::other(format!("DNS detour `{outbound}` could not be resolved"))
                })?;
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
                io::Error::other(format!(
                    "DNS detour `{outbound}` failed at {}: {}",
                    failure.stage, failure.error
                ))
            })?;
            extract_tcp_stream(established)
                .map(|result| result.upstream)
                .map_err(|error| {
                    io::Error::other(format!("DNS detour `{outbound}` failed: {error}"))
                })
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
                .exchange(endpoint, query, connector.egress_interface.generation())
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
