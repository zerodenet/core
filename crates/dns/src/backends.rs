//! Runtime implementations for configured DNS transports.

use std::io;
use std::net::SocketAddr;

use zero_config::DnsServerConfig;
use zero_traits::{AddressFamily, IpAddress};

use crate::message::{
    build_address_response, build_error_response, parse_question, parse_response,
    DEFAULT_NEGATIVE_TTL_SECONDS, RCODE_NOTIMP, RCODE_NXDOMAIN, TYPE_A, TYPE_AAAA,
};
use crate::system::TokioSystemResolver;
#[cfg(feature = "udp")]
use crate::udp::UdpDnsResolver;
use crate::{DnsConnectContext, DnsOutboundConnector};
mod lifecycle;
tokio::task_local! { pub(crate) static ATTEMPT_DEADLINE:tokio::time::Instant; }

pub(crate) enum ResolverBackend {
    System(TokioSystemResolver),
    #[cfg(feature = "udp")]
    Udp {
        resolver: UdpDnsResolver,
        tcp_addrs: Vec<SocketAddr>,
        egress: zero_platform_tokio::EgressInterfaceControl,
    },
    #[cfg(feature = "doh")]
    Doh(DohDnsResolver),
    #[cfg(feature = "dot")]
    Dot(DotDnsResolver),
    #[cfg(feature = "doq")]
    Doq(DoqDnsResolver),
}

impl ResolverBackend {
    pub(crate) fn build(
        server: &DnsServerConfig,
        egress: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        let _ = &egress;
        match server {
            DnsServerConfig::System => Ok(Self::System(TokioSystemResolver)),
            #[cfg(feature = "udp")]
            DnsServerConfig::Udp { port, .. } => {
                let addrs = server_endpoints(server, *port)?;
                Ok(Self::Udp {
                    resolver: UdpDnsResolver::new(addrs.clone(), egress.clone()),
                    tcp_addrs: addrs,
                    egress,
                })
            }
            #[cfg(not(feature = "udp"))]
            DnsServerConfig::Udp { .. } => Err(unsupported("UDP", "udp")),
            #[cfg(feature = "doh")]
            DnsServerConfig::Doh {
                host,
                port,
                path,
                bootstrap,
                server_name,
                ..
            } => Ok(Self::Doh(DohDnsResolver::new(
                host.clone(),
                *port,
                path.clone(),
                bootstrap.clone(),
                server_name.clone(),
                egress,
            )?)),
            #[cfg(not(feature = "doh"))]
            DnsServerConfig::Doh { .. } => Err(unsupported("DNS-over-HTTPS", "doh")),
            #[cfg(feature = "dot")]
            DnsServerConfig::Dot {
                host,
                port,
                bootstrap,
                server_name,
                ..
            } => Ok(Self::Dot(DotDnsResolver::new(
                host.clone(),
                *port,
                bootstrap.clone(),
                server_name.clone(),
                egress,
            )?)),
            #[cfg(not(feature = "dot"))]
            DnsServerConfig::Dot { .. } => Err(unsupported("DNS-over-TLS", "dot")),
            #[cfg(feature = "doq")]
            DnsServerConfig::Doq {
                host,
                port,
                bootstrap,
                server_name,
                ..
            } => Ok(Self::Doq(DoqDnsResolver::new(
                host.clone(),
                *port,
                bootstrap.clone(),
                server_name.clone(),
                egress,
            )?)),
            #[cfg(not(feature = "doq"))]
            DnsServerConfig::Doq { .. } => Err(unsupported("DNS-over-QUIC", "doq")),
        }
    }

    #[cfg(feature = "udp")]
    pub(crate) fn build_owned_udp(
        server: &DnsServerConfig,
        egress: zero_platform_tokio::EgressInterfaceControl,
        owned: std::sync::Arc<crate::udp::OwnedDnsUdp>,
    ) -> io::Result<Self> {
        let DnsServerConfig::Udp { port, .. } = server else {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "owned DNS UDP root does not authorize another transport",
            ));
        };
        let addrs = server_endpoints(server, *port)?;
        Ok(Self::Udp {
            resolver: UdpDnsResolver::new_owned(addrs.clone(), egress.clone(), owned)?,
            tcp_addrs: addrs,
            egress,
        })
    }

    #[cfg(feature = "udp")]
    pub(crate) fn owned_udp(&self) -> Option<&UdpDnsResolver> {
        match self {
            Self::Udp { resolver, .. } if resolver.is_owned() => Some(resolver),
            _ => None,
        }
    }

    pub(crate) async fn exchange(
        &self,
        query: &[u8],
        detour: Option<&str>,
        connector: Option<&dyn DnsOutboundConnector>,
        family: AddressFamily,
    ) -> io::Result<Vec<u8>> {
        match self {
            Self::System(resolver) => system_exchange(*resolver, query, family).await,
            #[cfg(feature = "udp")]
            Self::Udp {
                resolver,
                tcp_addrs,
                egress,
            } => {
                if let Some(outbound) = detour {
                    if let Some(connector) = connector {
                        match exchange_detoured_udp(tcp_addrs, query, outbound, connector).await {
                            Ok(response) if !parse_response(query, &response)?.truncated => {
                                return Ok(response);
                            }
                            Ok(_) => {}
                            Err(error) if error.kind() == io::ErrorKind::Unsupported => {}
                            Err(error) => return Err(error),
                        }
                    }
                    return exchange_tcp_many(tcp_addrs, query, egress, detour, connector).await;
                }
                let response = resolver.exchange(query).await?;
                if !parse_response(query, &response)?.truncated {
                    return Ok(response);
                }
                if resolver.is_owned() {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "truncated owned DNS UDP requires a separately authorized native TCP root",
                    ));
                }
                exchange_tcp_many(tcp_addrs, query, egress, None, None).await
            }
            #[cfg(feature = "doh")]
            Self::Doh(resolver) => resolver.exchange(query, detour, connector).await,
            #[cfg(feature = "dot")]
            Self::Dot(resolver) => resolver.exchange(query, detour, connector).await,
            #[cfg(feature = "doq")]
            Self::Doq(resolver) if detour.is_none() => {
                match connector.and_then(|connector| connector.quic_bidi()) {
                    Some(provider) => resolver.exchange_hosted(query, provider).await,
                    None => resolver.exchange(query).await,
                }
            }
            #[cfg(feature = "doq")]
            Self::Doq(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DoQ detour requires a proxy-aware UDP/QUIC carrier",
            )),
        }
    }

    pub(crate) fn register_owners(
        &self,
        owners: &std::sync::Arc<crate::transport_owners::TransportOwners>,
    ) -> io::Result<()> {
        #[cfg(feature = "doh")]
        if let Self::Doh(resolver) = self {
            owners.register_doh(resolver.owner.clone())?;
        }
        #[cfg(feature = "doq")]
        if let Self::Doq(resolver) = self {
            resolver.attach_registry(owners.clone());
        }
        let _ = owners;
        Ok(())
    }

    pub(crate) fn transport_name(&self) -> &'static str {
        match self {
            Self::System(_) => "system",
            #[cfg(feature = "udp")]
            Self::Udp { .. } => "udp",
            #[cfg(feature = "doh")]
            Self::Doh(_) => "doh",
            #[cfg(feature = "dot")]
            Self::Dot(_) => "dot",
            #[cfg(feature = "doq")]
            Self::Doq(_) => "doq",
        }
    }

    pub(crate) fn endpoint_labels(&self) -> Vec<String> {
        match self {
            Self::System(_) => vec!["system".to_owned()],
            #[cfg(feature = "udp")]
            Self::Udp { tcp_addrs, .. } => tcp_addrs.iter().map(ToString::to_string).collect(),
            #[cfg(feature = "doh")]
            Self::Doh(resolver) => resolver.endpoint_labels(),
            #[cfg(feature = "dot")]
            Self::Dot(resolver) => resolver.endpoint_labels(),
            #[cfg(feature = "doq")]
            Self::Doq(resolver) => resolver.endpoint_labels(),
        }
    }
}

#[cfg(feature = "udp")]
async fn exchange_detoured_udp(
    addrs: &[SocketAddr],
    query: &[u8],
    outbound: &str,
    connector: &dyn DnsOutboundConnector,
) -> io::Result<Vec<u8>> {
    let mut last_error = io::Error::new(
        io::ErrorKind::InvalidInput,
        "DNS UDP backend has no endpoint",
    );
    for addr in addrs {
        let response = connector
            .exchange_datagram(outbound.to_owned(), *addr, query.to_vec())
            .await;
        match response {
            Ok(response) => match parse_response(query, &response) {
                Ok(_) => return Ok(response),
                Err(error) => last_error = error,
            },
            Err(error) if error.kind() == io::ErrorKind::Unsupported => return Err(error),
            Err(error) => last_error = error,
        }
    }
    Err(last_error)
}

pub(crate) struct ResolvedAddresses {
    pub(crate) addresses: Vec<IpAddress>,
    pub(crate) ttl_seconds: u32,
}

async fn system_exchange(
    resolver: TokioSystemResolver,
    query: &[u8],
    family: AddressFamily,
) -> io::Result<Vec<u8>> {
    let question = parse_question(query)?;
    if !matches!(question.query_type, TYPE_A | TYPE_AAAA) {
        return Ok(build_error_response(query, RCODE_NOTIMP, false));
    }
    match resolver
        .resolve_type(&question.domain, question.query_type, family)
        .await
    {
        Ok(addresses) => Ok(build_address_response(
            query,
            &addresses,
            DEFAULT_NEGATIVE_TTL_SECONDS,
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok(build_error_response(query, RCODE_NXDOMAIN, false))
        }
        Err(error) => Err(error),
    }
}

fn server_endpoints(server: &DnsServerConfig, port: u16) -> io::Result<Vec<SocketAddr>> {
    let endpoints = server
        .endpoint_addresses()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        .into_iter()
        .map(|address| SocketAddr::new(address, port))
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "DNS backend has no endpoint",
        ));
    }
    Ok(endpoints)
}

#[allow(dead_code)]
fn unsupported(name: &str, feature: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("{name} backend is not compiled (enable feature `{feature}`)"),
    )
}

#[cfg(feature = "udp")]
async fn exchange_tcp_many(
    addrs: &[SocketAddr],
    query: &[u8],
    egress: &zero_platform_tokio::EgressInterfaceControl,
    detour: Option<&str>,
    connector: Option<&dyn DnsOutboundConnector>,
) -> io::Result<Vec<u8>> {
    let mut last_error = None;
    for addr in addrs {
        match exchange_tcp(*addr, query, egress, detour, connector).await {
            Ok(response) => return Ok(response),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "DNS TCP backend has no endpoint",
        )
    }))
}

#[cfg(feature = "udp")]
async fn exchange_tcp(
    addr: SocketAddr,
    query: &[u8],
    egress: &zero_platform_tokio::EgressInterfaceControl,
    detour: Option<&str>,
    connector: Option<&dyn DnsOutboundConnector>,
) -> io::Result<Vec<u8>> {
    let mut stream = connect_tcp(addr, egress, detour, connector).await?;
    write_framed(&mut stream, query).await?;
    read_framed(&mut stream).await
}

async fn connect_tcp(
    addr: SocketAddr,
    egress: &zero_platform_tokio::EgressInterfaceControl,
    detour: Option<&str>,
    connector: Option<&dyn DnsOutboundConnector>,
) -> io::Result<zero_platform_tokio::TcpRelayStream> {
    connect_tcp_carrier(addr, egress, detour, connector)
        .await
        .map(|carrier| carrier.stream)
}

async fn connect_tcp_carrier(
    addr: SocketAddr,
    egress: &zero_platform_tokio::EgressInterfaceControl,
    detour: Option<&str>,
    connector: Option<&dyn DnsOutboundConnector>,
) -> io::Result<crate::DnsOutboundCarrier> {
    if let Some(outbound) = detour {
        let connector = connector.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!("DNS detour `{outbound}` is unavailable outside the proxy runtime"),
            )
        })?;
        return connector
            .connect_carrier_with_context(
                outbound.to_owned(),
                addr,
                DnsConnectContext {
                    deadline: ATTEMPT_DEADLINE.try_with(|deadline| *deadline).ok(),
                    cancel: crate::coordinator::current_cancel(),
                },
            )
            .await;
    }

    let selection = egress.select_for_peer(addr);
    selection.ensure_connectable()?;
    let interface = selection.interface().cloned();
    tracing::debug!(
        peer = %addr,
        tun_active = selection.tun_active(),
        egress_generation = selection.generation(),
        binding_reason = selection.binding_reason().as_str(),
        route_lookup = selection.route_lookup_status().as_str(),
        route_source = ?selection.route_source(),
        interface_name = interface.as_ref().map(|value| value.name()),
        interface_index = interface.as_ref().map(|value| value.index()),
        "selected DNS physical egress"
    );
    zero_platform_tokio::TokioSocket::connect_addr_on(addr, interface.as_ref())
        .await
        .map(|stream| crate::DnsOutboundCarrier {
            stream: stream.into(),
            binding: None,
        })
}

async fn write_framed<S: tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    message: &[u8],
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let length: u16 = message
        .len()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "DNS message is too large"))?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(message).await?;
    stream.flush().await
}

async fn read_framed<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut length = [0_u8; 2];
    stream.read_exact(&mut length).await?;
    let length = u16::from_be_bytes(length) as usize;
    if length < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "framed DNS response is too short",
        ));
    }
    let mut response = vec![0_u8; length];
    stream.read_exact(&mut response).await?;
    Ok(response)
}

#[cfg(feature = "doh")]
mod doh;
#[cfg(feature = "doh")]
pub(crate) use doh::owner::DriverOwner as DohDriverOwner;
#[cfg(feature = "doh")]
pub(crate) use doh::DohDnsResolver;

#[cfg(feature = "dot")]
mod dot;
#[cfg(feature = "dot")]
use dot::DotDnsResolver;

#[cfg(feature = "doq")]
mod doq;
#[cfg(feature = "doq")]
pub(crate) use doq::owner::EndpointOwner as DoqEndpointOwner;
#[cfg(feature = "doq")]
use doq::DoqDnsResolver;
