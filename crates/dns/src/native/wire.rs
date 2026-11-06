use super::*;
use zero_config::DnsServerConfig;
use zero_platform_tokio::controlled_carrier::loan::LoanOwner;
mod attempt;
#[cfg(feature = "doq")]
mod quic;
#[cfg(feature = "dot")]
mod tls;
pub(crate) use attempt::Attempt;
struct Query<'a> {
    bytes: &'a [u8],
    caller: TaskJoinWitness,
    cancel: CancellationToken,
    end: Instant,
}

/// Pure admission for native Node resolution; performs no query I/O.
pub fn validate_node_server(server: &DnsServerConfig) -> io::Result<()> {
    if server.detour().is_some() {
        return Err(unsupported("native Node DNS cannot authorize a detour"));
    }
    if matches!(server, DnsServerConfig::System) {
        return Ok(());
    }
    if validated_peers(server)?.is_empty() {
        return Err(unsupported("node DNS has no native peers"));
    }
    Ok(())
}
pub fn validate_node_config(config: Option<&zero_config::DnsConfig>) -> io::Result<()> {
    let config = config
        .filter(|config| !config.servers.is_empty())
        .ok_or_else(|| unsupported("node DNS requires configured native servers"))?;
    let tags = match config.policy.node_server.as_deref() {
        Some(selected) => std::iter::once(selected)
            .chain(
                config
                    .policy
                    .node_fallback_servers
                    .iter()
                    .map(String::as_str),
            )
            .collect::<Vec<_>>(),
        None => std::iter::once(config.default_server.as_str())
            .chain(config.policy.fallback_servers.iter().map(String::as_str))
            // Without an explicit Node policy, Node resolution follows the
            // same real domain dispatcher as ordinary resolution.
            .chain(config.dispatch.iter().map(|rule| rule.server.as_str()))
            .collect(),
    };
    let mut seen = std::collections::BTreeSet::new();
    for tag in tags {
        if !seen.insert(tag) {
            continue;
        }
        let server = config.servers.get(tag).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("native Node DNS selected undefined backend `{tag}`"),
            )
        })?;
        validate_node_server(server)?;
    }
    Ok(())
}
pub fn native_endpoints(
    config: Option<&zero_config::DnsConfig>,
) -> io::Result<Vec<(SocketAddr, bool)>> {
    let mut peers = std::collections::BTreeSet::new();
    for server in config
        .into_iter()
        .flat_map(|config| config.servers.values())
    {
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
#[allow(clippy::too_many_arguments)]
pub(super) async fn exchange(
    provider: &dyn NativeDnsProvider,
    config: &DnsServerConfig,
    backend: &crate::backends::ResolverBackend,
    query: &[u8],
    family: zero_traits::AddressFamily,
    caller: TaskJoinWitness,
    cancel: CancellationToken,
    end: Instant,
) -> Attempt {
    // Existing DoQ detours have never had a proxy-aware QUIC carrier. Do not
    // turn host-only execution into a direct QUIC bypass of that detour.
    if matches!(config, DnsServerConfig::Doq { .. }) && config.detour().is_some() {
        return Attempt::blocked(unsupported(
            "DoQ detour requires a proxy-aware UDP/QUIC carrier",
        ));
    }
    if !matches!(config, DnsServerConfig::Udp { .. })
        && config
            .detour()
            .is_some_and(|outbound| !provider.owns_detour(outbound))
    {
        return Attempt::blocked(unsupported(
            "DNS detour has no actual standard Stream owner",
        ));
    }
    if matches!(config, DnsServerConfig::System) {
        let question = match crate::message::parse_question(query) {
            Ok(question) => question,
            Err(source) => return Attempt::blocked(source),
        };
        let lookup = match provider.system(question.domain, caller, cancel, end).await {
            Ok(lookup) => lookup,
            Err(source) => return Attempt::blocked(source),
        };
        return Attempt::system(lookup, query, question.query_type, family);
    }
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
    let port = config
        .port()
        .ok_or_else(|| unsupported("OS resolver has no native original-close provider"))?;
    #[cfg(not(feature = "doq"))]
    if matches!(config, DnsServerConfig::Doq { .. }) {
        return Err(unsupported(
            "native DoQ requires its original QUIC endpoint provider",
        ));
    }
    #[cfg(not(feature = "dot"))]
    if matches!(config, DnsServerConfig::Dot { .. }) {
        return Err(unsupported("native DoT feature is disabled"));
    }
    #[cfg(not(feature = "doh"))]
    if matches!(config, DnsServerConfig::Doh { .. }) {
        return Err(unsupported("native DoH feature is disabled"));
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
    #[cfg(not(any(feature = "dot", feature = "doq")))]
    let _ = backend;
    let end = *end;
    #[cfg(feature = "doq")]
    if matches!(config, DnsServerConfig::Doq { .. }) {
        return Box::pin(quic::exchange(provider, backend, request, peer)).await;
    }
    let mut stream = None;
    if matches!(config, DnsServerConfig::Udp { .. }) {
        let opening = async {
            match config.detour() {
                Some(outbound) => {
                    provider
                        .detour_udp(
                            outbound.to_owned(),
                            peer,
                            caller.clone(),
                            cancel.clone(),
                            end,
                        )
                        .await
                }
                None => provider
                    .datagram(peer, caller.clone(), cancel.clone(), end)
                    .await
                    .map(NativeDnsUdpLoan::Datagram),
            }
        };
        let loan = match OwnedDnsQueryTasks::native_io(opening)
            .await
            .and_then(std::convert::identity)
        {
            Ok(loan) => loan,
            Err(source) if config.detour().is_some() => {
                return attempt::failed_service(provider, caller.clone(), end, source).await;
            }
            Err(source) => return Attempt::open_failed(source),
        };
        match loan {
            NativeDnsUdpLoan::Stream(loan) => stream = Some(loan),
            NativeDnsUdpLoan::Datagram(loan) => {
                let response = exchange_datagram(loan, request).await;
                if !response.is_truncated(query) {
                    return response;
                }
                // The original UDP controller has confirmed close before a
                // separately selected TCP carrier can acquire any resource.
            }
        }
    }
    let opened = match stream {
        Some(loan) => Ok(loan),
        None => OwnedDnsQueryTasks::native_io(async {
            match config.detour() {
                Some(outbound) => provider
                    .detour_stream(
                        outbound.to_owned(),
                        peer,
                        caller.clone(),
                        cancel.clone(),
                        end,
                    )
                    .await
                    .map(|(loan, _)| loan),
                None => {
                    provider
                        .stream(peer, caller.clone(), cancel.clone(), end)
                        .await
                }
            }
        })
        .await
        .and_then(std::convert::identity),
    };
    let loan = match opened {
        Ok(loan) => loan,
        Err(source) if config.detour().is_some() => {
            return attempt::failed_service(provider, caller.clone(), end, source).await
        }
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
            #[cfg(feature = "dot")]
            _ => Box::pin(tls::exchange(config, backend, io, query)).await,
            #[cfg(not(feature = "dot"))]
            _ => Err(unsupported("native DoT feature is disabled")),
        }
    };
    let response = attempt(end, async { OwnedDnsQueryTasks::native_io(work).await? }).await;
    // The inline operation owns the stream, including TLS/H2 on every branch.
    attempt::settle(&mut owner, end, response).await
}
async fn exchange_datagram(loan: DatagramLoan, request: &Query<'_>) -> Attempt {
    let (io, mut owner) = (loan.io, loan.owner);
    let end = request.end;
    if let Err(source) = OwnedDnsQueryTasks::record_native(owner.original()) {
        drop(io);
        return attempt::settle(&mut owner, end, Err(source)).await;
    }
    let work = async {
        io.send(request.bytes).await?;
        let mut bytes = vec![0; 65536];
        loop {
            let size = io.receive(&mut bytes).await?;
            if crate::message::parse_response(request.bytes, &bytes[..size]).is_ok() {
                return Ok(bytes[..size].to_vec());
            }
        }
    };
    let response = attempt(end, async { OwnedDnsQueryTasks::native_io(work).await? }).await;
    drop(io);
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
