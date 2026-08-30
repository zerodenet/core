use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use zero_platform_tokio::{EgressInterfaceControl, EgressSelection, TcpConnectError, TokioSocket};

const CANDIDATE_DELAY: Duration = Duration::from_millis(250);
const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const MAX_RECORDED_CONNECTION_ATTEMPTS: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TcpDialAttempt {
    pub(super) remote: SocketAddr,
    pub(super) local_addr: Option<SocketAddr>,
    pub(super) stage: &'static str,
    pub(super) outcome: &'static str,
    pub(super) interface_bound: bool,
    pub(super) error_kind: Option<&'static str>,
    pub(super) os_error: Option<i32>,
    pub(super) error: Option<String>,
    candidate_index: usize,
}

#[derive(Debug)]
pub(super) struct TcpDialSuccess {
    pub(super) socket: TokioSocket,
    pub(super) remote: SocketAddr,
    pub(super) resolved_candidates: Vec<SocketAddr>,
    pub(super) selection: EgressSelection,
    pub(super) attempts: Vec<TcpDialAttempt>,
    candidate_index: usize,
    local_addr: Option<SocketAddr>,
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
    pub(super) attempts: Vec<TcpDialAttempt>,
    candidate_index: usize,
}

pub(super) async fn dial_tcp_candidates(
    candidates: Vec<SocketAddr>,
    egress: &EgressInterfaceControl,
) -> Result<TcpDialSuccess, Box<TcpDialFailure>> {
    let resolved_candidates = interleave_address_families(candidates);
    dial_tcp_candidates_with_history(
        resolved_candidates.clone(),
        egress,
        0,
        resolved_candidates,
        Vec::new(),
    )
    .await
}

/// Continue one failed dial with newly discovered candidates without retrying
/// any endpoint that already completed. The returned observation remains one
/// deterministic candidate/attempt timeline across both phases.
pub(super) async fn dial_tcp_fallback_candidates(
    previous: Box<TcpDialFailure>,
    candidates: Vec<SocketAddr>,
    egress: &EgressInterfaceControl,
) -> Result<TcpDialSuccess, Box<TcpDialFailure>> {
    let mut resolved_candidates = previous.resolved_candidates.clone();
    let fallback_candidates = interleave_address_families(candidates)
        .into_iter()
        .filter(|candidate| !resolved_candidates.contains(candidate))
        .collect::<Vec<_>>();
    if fallback_candidates.is_empty() {
        return Err(previous);
    }

    let candidate_index_offset = resolved_candidates.len();
    resolved_candidates.extend(fallback_candidates.iter().copied());
    dial_tcp_candidates_with_history(
        fallback_candidates,
        egress,
        candidate_index_offset,
        resolved_candidates,
        previous.attempts,
    )
    .await
}

async fn dial_tcp_candidates_with_history(
    candidates: Vec<SocketAddr>,
    egress: &EgressInterfaceControl,
    candidate_index_offset: usize,
    resolved_candidates: Vec<SocketAddr>,
    mut completed_attempts: Vec<TcpDialAttempt>,
) -> Result<TcpDialSuccess, Box<TcpDialFailure>> {
    let mut next_candidate = candidates
        .iter()
        .copied()
        .enumerate()
        .map(|(index, candidate)| (candidate_index_offset + index, candidate));
    let (first_index, first) = next_candidate
        .next()
        .expect("dial candidates are non-empty");
    let mut attempts = FuturesUnordered::new();
    attempts.push(dial_tcp_candidate(first_index, first, egress.clone()));
    let mut last_failure = None;

    loop {
        if let Some((candidate_index, candidate)) = next_candidate.next() {
            tokio::select! {
                result = attempts.next() => {
                    match result.expect("at least one dial attempt is active") {
                        Ok(success) => {
                            return Ok(complete_success(
                                success,
                                resolved_candidates,
                                completed_attempts,
                            ));
                        }
                        Err(failure) => {
                            completed_attempts.push(failure.observation());
                            last_failure = Some(failure);
                            attempts.push(dial_tcp_candidate(
                                candidate_index,
                                candidate,
                                egress.clone(),
                            ));
                        }
                    }
                }
                _ = tokio::time::sleep(CANDIDATE_DELAY) => {
                    attempts.push(dial_tcp_candidate(
                        candidate_index,
                        candidate,
                        egress.clone(),
                    ));
                }
            }
            continue;
        }

        match attempts.next().await {
            Some(Ok(success)) => {
                return Ok(complete_success(
                    success,
                    resolved_candidates,
                    completed_attempts,
                ));
            }
            Some(Err(failure)) => {
                completed_attempts.push(failure.observation());
                last_failure = Some(failure);
            }
            None => {
                let mut failure = last_failure.expect("at least one dial candidate was attempted");
                failure.resolved_candidates = resolved_candidates;
                failure.attempts = bounded_attempts(completed_attempts, failure.candidate_index);
                return Err(failure);
            }
        }
    }
}

async fn dial_tcp_candidate(
    candidate_index: usize,
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
            attempts: Vec::new(),
            candidate_index,
        }));
    }

    match tokio::time::timeout(
        CANDIDATE_TIMEOUT,
        TokioSocket::connect_addr_on_observed(remote, selection.interface()),
    )
    .await
    {
        Ok(Ok(socket)) => {
            let local_addr = socket.local_addr().ok();
            Ok(TcpDialSuccess {
                socket,
                remote,
                resolved_candidates: Vec::new(),
                selection,
                attempts: Vec::new(),
                candidate_index,
                local_addr,
            })
        }
        Ok(Err(error)) => Err(connect_failure(candidate_index, remote, selection, error)),
        Err(_) => Err(Box::new(TcpDialFailure {
            remote,
            resolved_candidates: Vec::new(),
            interface_bound: selection.interface().is_some(),
            selection,
            stage: "connect_timeout",
            local_addr: None,
            error: io::Error::new(io::ErrorKind::TimedOut, "TCP candidate connect timed out"),
            attempts: Vec::new(),
            candidate_index,
        })),
    }
}

fn connect_failure(
    candidate_index: usize,
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
        attempts: Vec::new(),
        candidate_index,
    })
}

impl TcpDialSuccess {
    fn observation(&self) -> TcpDialAttempt {
        TcpDialAttempt {
            remote: self.remote,
            local_addr: self.local_addr,
            stage: "connected",
            outcome: "connected",
            interface_bound: self.selection.interface().is_some(),
            error_kind: None,
            os_error: None,
            error: None,
            candidate_index: self.candidate_index,
        }
    }
}

impl TcpDialFailure {
    fn observation(&self) -> TcpDialAttempt {
        TcpDialAttempt {
            remote: self.remote,
            local_addr: self.local_addr,
            stage: self.stage,
            outcome: "failed",
            interface_bound: self.interface_bound,
            error_kind: Some(io_error_kind_name(self.error.kind())),
            os_error: self.error.raw_os_error(),
            error: Some(self.error.to_string()),
            candidate_index: self.candidate_index,
        }
    }
}

fn complete_success(
    mut success: TcpDialSuccess,
    resolved_candidates: Vec<SocketAddr>,
    mut completed_attempts: Vec<TcpDialAttempt>,
) -> TcpDialSuccess {
    completed_attempts.push(success.observation());
    success.resolved_candidates = resolved_candidates;
    success.attempts = bounded_attempts(completed_attempts, success.candidate_index);
    success
}

fn bounded_attempts(
    mut attempts: Vec<TcpDialAttempt>,
    terminal_candidate_index: usize,
) -> Vec<TcpDialAttempt> {
    attempts.sort_by_key(|attempt| attempt.candidate_index);
    if attempts.len() <= MAX_RECORDED_CONNECTION_ATTEMPTS {
        return attempts;
    }

    let terminal = attempts
        .iter()
        .find(|attempt| attempt.candidate_index == terminal_candidate_index)
        .cloned();
    attempts.truncate(MAX_RECORDED_CONNECTION_ATTEMPTS);
    if let Some(terminal) = terminal.filter(|terminal| {
        !attempts
            .iter()
            .any(|attempt| attempt.candidate_index == terminal.candidate_index)
    }) {
        attempts.pop();
        attempts.push(terminal);
        attempts.sort_by_key(|attempt| attempt.candidate_index);
    }
    attempts
}

fn io_error_kind_name(kind: io::ErrorKind) -> &'static str {
    match kind {
        io::ErrorKind::NotFound => "not_found",
        io::ErrorKind::PermissionDenied => "permission_denied",
        io::ErrorKind::ConnectionRefused => "connection_refused",
        io::ErrorKind::ConnectionReset => "connection_reset",
        io::ErrorKind::HostUnreachable => "host_unreachable",
        io::ErrorKind::NetworkUnreachable => "network_unreachable",
        io::ErrorKind::ConnectionAborted => "connection_aborted",
        io::ErrorKind::NotConnected => "not_connected",
        io::ErrorKind::AddrInUse => "address_in_use",
        io::ErrorKind::AddrNotAvailable => "address_not_available",
        io::ErrorKind::NetworkDown => "network_down",
        io::ErrorKind::BrokenPipe => "broken_pipe",
        io::ErrorKind::WouldBlock => "would_block",
        io::ErrorKind::InvalidInput => "invalid_input",
        io::ErrorKind::InvalidData => "invalid_data",
        io::ErrorKind::TimedOut => "timed_out",
        io::ErrorKind::WriteZero => "write_zero",
        io::ErrorKind::Interrupted => "interrupted",
        io::ErrorKind::Unsupported => "unsupported",
        io::ErrorKind::UnexpectedEof => "unexpected_eof",
        io::ErrorKind::OutOfMemory => "out_of_memory",
        io::ErrorKind::Other => "other",
        _ => "other",
    }
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
