use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use zero_platform_tokio::{EgressInterfaceControl, EgressSelection, TcpConnectError, TokioSocket};

const CANDIDATE_DELAY: Duration = Duration::from_millis(250);
const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) struct TcpDialSuccess {
    pub(super) socket: TokioSocket,
    pub(super) remote: SocketAddr,
    pub(super) resolved_candidates: Vec<SocketAddr>,
    pub(super) selection: EgressSelection,
}

#[derive(Debug)]
pub(super) struct TcpDialFailure {
    pub(super) remote: SocketAddr,
    pub(super) resolved_candidates: Vec<SocketAddr>,
    pub(super) selection: EgressSelection,
    pub(super) stage: &'static str,
    pub(super) interface_bound: bool,
    pub(super) local_addr: Option<SocketAddr>,
    pub(super) error: io::Error,
}

pub(super) async fn dial_tcp_candidates(
    candidates: Vec<SocketAddr>,
    egress: &EgressInterfaceControl,
) -> Result<TcpDialSuccess, Box<TcpDialFailure>> {
    let resolved_candidates = interleave_address_families(candidates);
    let mut next_candidate = resolved_candidates.iter().copied();
    let first = next_candidate
        .next()
        .expect("dial candidates are non-empty");
    let mut attempts = FuturesUnordered::new();
    attempts.push(dial_tcp_candidate(first, egress.clone()));
    let mut last_failure = None;

    loop {
        if let Some(candidate) = next_candidate.next() {
            tokio::select! {
                result = attempts.next() => {
                    match result.expect("at least one dial attempt is active") {
                        Ok(mut success) => {
                            success.resolved_candidates = resolved_candidates.clone();
                            return Ok(success);
                        }
                        Err(failure) => {
                            last_failure = Some(failure);
                            attempts.push(dial_tcp_candidate(candidate, egress.clone()));
                        }
                    }
                }
                _ = tokio::time::sleep(CANDIDATE_DELAY) => {
                    attempts.push(dial_tcp_candidate(candidate, egress.clone()));
                }
            }
            continue;
        }

        match attempts.next().await {
            Some(Ok(mut success)) => {
                success.resolved_candidates = resolved_candidates.clone();
                return Ok(success);
            }
            Some(Err(failure)) => last_failure = Some(failure),
            None => {
                let mut failure = last_failure.expect("at least one dial candidate was attempted");
                failure.resolved_candidates = resolved_candidates.clone();
                return Err(failure);
            }
        }
    }
}

async fn dial_tcp_candidate(
    remote: SocketAddr,
    egress: EgressInterfaceControl,
) -> Result<TcpDialSuccess, Box<TcpDialFailure>> {
    let selection = egress.select_for_peer(remote);
    if let Err(error) = selection.ensure_connectable() {
        return Err(Box::new(TcpDialFailure {
            remote,
            resolved_candidates: Vec::new(),
            selection,
            stage: "select_egress",
            interface_bound: false,
            local_addr: None,
            error,
        }));
    }

    match tokio::time::timeout(
        CANDIDATE_TIMEOUT,
        TokioSocket::connect_addr_on_observed(remote, selection.interface()),
    )
    .await
    {
        Ok(Ok(socket)) => Ok(TcpDialSuccess {
            socket,
            remote,
            resolved_candidates: Vec::new(),
            selection,
        }),
        Ok(Err(error)) => Err(connect_failure(remote, selection, error)),
        Err(_) => Err(Box::new(TcpDialFailure {
            remote,
            resolved_candidates: Vec::new(),
            interface_bound: selection.interface().is_some(),
            selection,
            stage: "connect_timeout",
            local_addr: None,
            error: io::Error::new(io::ErrorKind::TimedOut, "TCP candidate connect timed out"),
        })),
    }
}

fn connect_failure(
    remote: SocketAddr,
    selection: EgressSelection,
    error: TcpConnectError,
) -> Box<TcpDialFailure> {
    Box::new(TcpDialFailure {
        remote,
        resolved_candidates: Vec::new(),
        selection,
        stage: error.stage(),
        interface_bound: error.interface_bound(),
        local_addr: error.local_addr(),
        error: error.into_inner(),
    })
}

/// Preserve resolver order within each family while making the other family
/// eligible after one connection-attempt delay. Duplicate answers are removed
/// so a DNS response cannot waste the bounded dial window on repeated peers.
pub(super) fn interleave_address_families(candidates: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let Some(first) = candidates.first() else {
        return candidates;
    };
    let first_is_ipv6 = first.is_ipv6();
    let mut ipv4 = Vec::new();
    let mut ipv6 = Vec::new();
    for candidate in candidates {
        let family = if candidate.is_ipv6() {
            &mut ipv6
        } else {
            &mut ipv4
        };
        if !family.contains(&candidate) {
            family.push(candidate);
        }
    }
    let (preferred, alternate) = if first_is_ipv6 {
        (ipv6, ipv4)
    } else {
        (ipv4, ipv6)
    };

    let mut ordered = Vec::with_capacity(preferred.len() + alternate.len());
    let mut preferred = preferred.into_iter();
    let mut alternate = alternate.into_iter();
    loop {
        match (preferred.next(), alternate.next()) {
            (None, None) => break,
            (preferred, alternate) => {
                ordered.extend(preferred);
                ordered.extend(alternate);
            }
        }
    }
    ordered
}
