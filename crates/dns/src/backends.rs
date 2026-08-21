//! Runtime implementations for configured DNS transports.

use std::io;
use std::net::SocketAddr;

use zero_config::DnsServerConfig;
use zero_traits::IpAddress;

use crate::message::{
    build_address_response, build_error_response, build_query, parse_question, parse_response,
    DEFAULT_NEGATIVE_TTL_SECONDS, RCODE_NOERROR, RCODE_NOTIMP, RCODE_NXDOMAIN, TYPE_A, TYPE_AAAA,
};
use crate::system::TokioSystemResolver;
#[cfg(feature = "udp")]
use crate::udp::UdpDnsResolver;

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

    pub(crate) async fn exchange(&self, query: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Self::System(resolver) => system_exchange(*resolver, query).await,
            #[cfg(feature = "udp")]
            Self::Udp {
                resolver,
                tcp_addrs,
                egress,
            } => {
                let response = resolver.exchange(query).await?;
                if !parse_response(query, &response)?.truncated {
                    return Ok(response);
                }
                exchange_tcp_many(tcp_addrs, query, egress).await
            }
            #[cfg(feature = "doh")]
            Self::Doh(resolver) => resolver.exchange(query).await,
            #[cfg(feature = "dot")]
            Self::Dot(resolver) => resolver.exchange(query).await,
            #[cfg(feature = "doq")]
            Self::Doq(resolver) => resolver.exchange(query).await,
        }
    }

    pub(crate) async fn resolve_type(
        &self,
        domain: &str,
        query_type: u16,
    ) -> io::Result<ResolvedAddresses> {
        if let Self::System(resolver) = self {
            let addresses = resolver.resolve_type(domain, query_type).await?;
            return Ok(ResolvedAddresses {
                addresses,
                ttl_seconds: DEFAULT_NEGATIVE_TTL_SECONDS,
            });
        }
        let query = build_query(domain, query_type)?;
        let response = self.exchange(&query).await?;
        let parsed = parse_response(&query, &response)?;
        match parsed.response_code {
            RCODE_NOERROR => Ok(ResolvedAddresses {
                addresses: parsed.addresses,
                ttl_seconds: parsed
                    .min_ttl_seconds
                    .unwrap_or(DEFAULT_NEGATIVE_TTL_SECONDS),
            }),
            RCODE_NXDOMAIN => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("DNS name `{domain}` does not exist"),
            )),
            code => Err(io::Error::other(format!(
                "DNS server returned response code {code} for `{domain}`"
            ))),
        }
    }
}

pub(crate) struct ResolvedAddresses {
    pub(crate) addresses: Vec<IpAddress>,
    pub(crate) ttl_seconds: u32,
}

async fn system_exchange(resolver: TokioSystemResolver, query: &[u8]) -> io::Result<Vec<u8>> {
    let question = parse_question(query)?;
    if !matches!(question.query_type, TYPE_A | TYPE_AAAA) {
        return Ok(build_error_response(query, RCODE_NOTIMP, false));
    }
    match resolver
        .resolve_type(&question.domain, question.query_type)
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
) -> io::Result<Vec<u8>> {
    let mut last_error = None;
    for addr in addrs {
        match exchange_tcp(*addr, query, egress).await {
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
) -> io::Result<Vec<u8>> {
    let interface = egress.current_for_peer(addr);
    let mut stream =
        zero_platform_tokio::TokioSocket::connect_addr_on(addr, interface.as_ref()).await?;
    write_framed(&mut stream, query).await?;
    read_framed(&mut stream).await
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
use doh::DohDnsResolver;

#[cfg(feature = "dot")]
mod dot;
#[cfg(feature = "dot")]
use dot::DotDnsResolver;

#[cfg(feature = "doq")]
mod doq;
#[cfg(feature = "doq")]
use doq::DoqDnsResolver;
