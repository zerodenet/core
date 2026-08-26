use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Weak};

use zero_core::{Address, Network, ProtocolType, Session};
use zero_dns::{DnsOutboundConnectFuture, DnsOutboundConnector, DnsSystem};
use zero_engine::Engine;

use crate::inventory::ProtocolInventory;
use crate::protocol_registry::TcpRuntimeServices;
use crate::runtime::principal_rate_limit::PrincipalRateLimitRegistry;
use crate::transport::extract_tcp_stream;

#[derive(Clone)]
pub(super) struct ProxyDnsOutboundConnector {
    engine: Engine,
    resolver: Weak<DnsSystem>,
    protocols: ProtocolInventory,
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
            protocols,
            egress_interface,
            principal_rate_limits,
        }
    }

    fn runtime_services(&self) -> io::Result<TcpRuntimeServices> {
        let resolver = self.resolver.upgrade().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "DNS runtime is shutting down")
        })?;
        Ok(TcpRuntimeServices::new(
            self.engine.clone(),
            self.engine.runtime_snapshot(),
            resolver,
            self.protocols.clone(),
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
}

fn endpoint_address(address: IpAddr) -> Address {
    match address {
        IpAddr::V4(address) => Address::Ipv4(address.octets()),
        IpAddr::V6(address) => Address::Ipv6(address.octets()),
    }
}

#[cfg(all(test, feature = "dns"))]
mod tests;
