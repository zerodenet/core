use super::*;
use zero_config::DnsServerConfig;
use zero_platform_tokio::controlled_carrier::loan::LoanOwner;
mod attempt;
#[cfg(feature = "doq")]
mod quic;
#[cfg(any(feature = "dot", feature = "doh"))]
mod tls;
pub(crate) use attempt::Attempt;
struct Query<'a> {
    bytes: &'a [u8],
    caller: TaskJoinWitness,
    cancel: CancellationToken,
    end: Instant,
}

pub fn native_endpoints(
    config: Option<&zero_config::DnsConfig>,
) -> io::Result<Vec<(SocketAddr, bool)>> {
    let mut peers = std::collections::BTreeSet::new();
    for server in config
        .into_iter()
        .flat_map(|config| config.servers.values())
    {
        if server.detour().is_some() {
            continue;
        }
        if let Some(port) = server.port() {
            for peer in crate::backends::server_endpoints(server, port)? {
                if matches!(server, DnsServerConfig::Udp { .. }) {
                    peers.insert((peer, true));
                }
                if !matches!(server, DnsServerConfig::Doq { .. }) {
                    peers.insert((peer, false));
                }
            }
        }
    }
    Ok(peers.into_iter().collect())
}
#[cfg(feature = "doq")]
pub fn native_quic_endpoints(
    config: Option<&zero_config::DnsConfig>,
) -> io::Result<Vec<SocketAddr>> {
    let mut peers = std::collections::BTreeSet::new();
    for server in config
        .into_iter()
        .flat_map(|config| config.servers.values())
    {
        if let DnsServerConfig::Doq { port, .. } = server {
            if server.detour().is_none() {
                peers.extend(crate::backends::server_endpoints(server, *port)?);
            }
        }
    }
    Ok(peers.into_iter().collect())
}
pub(super) async fn exchange(
    provider: &dyn NativeDnsProvider,
    config: &DnsServerConfig,
    backend: &crate::backends::ResolverBackend,
    query: &[u8],
    caller: TaskJoinWitness,
    cancel: CancellationToken,
    end: Instant,
) -> Attempt {
    let request = Query {
        bytes: query,
        caller,
        cancel,
        end,
    };
    match validated_peers(config) {
        Ok(peers) => {
            let mut last = None;
            for peer in peers {
                let attempt = exchange_peer(provider, config, backend, &request, peer).await;
                let retry = attempt.can_retry()
                    && attempt.failed()
                    && Instant::now() < end
                    && !request.cancel.is_cancelled();
                if !retry {
                    return attempt;
                }
                last = Some(attempt);
            }
            last.unwrap_or_else(|| {
                Attempt::blocked(unsupported(
                    "native DNS requires a literal or bootstrap peer",
                ))
            })
        }
        Err(source) => Attempt::blocked(source),
    }
}
fn validated_peers(config: &DnsServerConfig) -> io::Result<Vec<SocketAddr>> {
    if config.detour().is_some() {
        return Err(unsupported("native DNS root cannot authorize a detour"));
    }
    let port = config
        .port()
        .ok_or_else(|| unsupported("OS resolver has no native original-close provider"))?;
    #[cfg(not(feature = "doq"))]
    if matches!(config, DnsServerConfig::Doq { .. }) {
        return Err(unsupported(
            "native DoQ requires its original QUIC endpoint provider",
        ));
    }
    crate::backends::server_endpoints(config, port)
}
async fn exchange_peer(
    provider: &dyn NativeDnsProvider,
    config: &DnsServerConfig,
    backend: &crate::backends::ResolverBackend,
    request: &Query<'_>,
    peer: SocketAddr,
) -> Attempt {
    let Query {
        bytes: query,
        caller,
        cancel,
        end,
    } = request;
    #[cfg(not(any(feature = "dot", feature = "doh")))]
    let _ = backend;
    let end = *end;
    #[cfg(feature = "doq")]
    if matches!(config, DnsServerConfig::Doq { .. }) {
        return Box::pin(quic::exchange(provider, backend, request, peer)).await;
    }
    if matches!(config, DnsServerConfig::Udp { .. }) {
        let loan = match provider
            .datagram(peer, caller.clone(), cancel.clone(), end)
            .await
        {
            Ok(loan) => loan,
            Err(source) => return Attempt::blocked(source),
        };
        let (io, mut owner) = (loan.io, loan.owner);
        if let Err(source) = OwnedDnsQueryTasks::record_native(owner.original()) {
            drop(io);
            return attempt::settle(&mut owner, end, Err(source)).await;
        }
        let work = async {
            io.send(query).await?;
            let mut bytes = vec![0; 65536];
            loop {
                let size = io.receive(&mut bytes).await?;
                if crate::message::parse_response(query, &bytes[..size]).is_ok() {
                    return Ok(bytes[..size].to_vec());
                }
            }
        };
        let response = attempt(end, async { OwnedDnsQueryTasks::native_io(work).await? }).await;
        drop(io);
        let response = attempt::settle(&mut owner, end, response).await;
        if !response.is_truncated(query) {
            return response;
        }
        // The original UDP controller has confirmed close before TCP exists.
    }
    let loan = match provider
        .stream(peer, caller.clone(), cancel.clone(), end)
        .await
    {
        Ok(loan) => loan,
        Err(source) => return Attempt::open_failed(source),
    };
    let (mut io, mut owner) = (loan.io, loan.owner);
    if let Err(source) = OwnedDnsQueryTasks::record_native(owner.original()) {
        drop(io);
        return attempt::settle(&mut owner, end, Err(source)).await;
    }
    let work = async move {
        match config {
            DnsServerConfig::Udp { .. } => {
                crate::backends::write_framed(&mut io, query).await?;
                crate::backends::read_framed(&mut io).await
            }
            #[cfg(any(feature = "dot", feature = "doh"))]
            _ => Box::pin(tls::exchange(config, backend, io, query)).await,
            #[cfg(not(any(feature = "dot", feature = "doh")))]
            _ => Err(unsupported("native encrypted DNS feature is disabled")),
        }
    };
    let response = attempt(end, async { OwnedDnsQueryTasks::native_io(work).await? }).await;
    // The inline operation owns the stream, including TLS/H2 on every branch.
    attempt::settle(&mut owner, end, response).await
}
fn unsupported(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, message)
}
async fn attempt<T>(
    end: Instant,
    work: impl std::future::Future<Output = io::Result<T>>,
) -> io::Result<T> {
    tokio::time::timeout_at(end, work)
        .await
        .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
}
