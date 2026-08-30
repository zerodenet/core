use std::collections::HashSet;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use zero_connector::{EventSinkTcpConnectFuture, EventSinkTcpDialer, EventSinkTcpStream};
use zero_platform_tokio::{EgressInterface, EgressInterfaceControl, TokioSocket};

#[cfg(test)]
mod tests;

const CANDIDATE_DELAY: Duration = Duration::from_millis(250);
const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) struct ApplicationEventSinkTcpDialer {
    egress: EgressInterfaceControl,
}

impl ApplicationEventSinkTcpDialer {
    pub(super) fn new(egress: EgressInterfaceControl) -> Self {
        Self { egress }
    }
}

impl EventSinkTcpDialer for ApplicationEventSinkTcpDialer {
    fn connect(&self, host: String, port: u16) -> EventSinkTcpConnectFuture {
        let egress = self.egress.clone();
        Box::pin(async move {
            let candidates = resolve_candidates(&host, port).await?;
            let socket = connect_candidates(candidates, egress).await?;
            Ok(Box::new(socket) as Box<dyn EventSinkTcpStream>)
        })
    }
}

async fn resolve_candidates(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    let mut unique = HashSet::new();
    let candidates = tokio::net::lookup_host((host, port))
        .await?
        .filter(|candidate| unique.insert(*candidate))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("webhook host `{host}` resolved to no addresses"),
        ));
    }
    Ok(candidates)
}

async fn connect_candidates(
    candidates: Vec<SocketAddr>,
    egress: EgressInterfaceControl,
) -> io::Result<TokioSocket> {
    let mut candidates = candidates.into_iter();
    let first = candidates
        .next()
        .expect("resolved webhook candidates are non-empty");
    let mut attempts = tokio::task::JoinSet::new();
    attempts.spawn(connect_candidate(first, egress.clone()));
    let mut last_error = None;

    loop {
        if let Some(candidate) = candidates.next() {
            tokio::select! {
                result = attempts.join_next() => {
                    match completed_attempt(result) {
                        Ok(socket) => return Ok(socket),
                        Err(error) => {
                            last_error = Some(error);
                            attempts.spawn(connect_candidate(candidate, egress.clone()));
                        }
                    }
                }
                _ = tokio::time::sleep(CANDIDATE_DELAY) => {
                    attempts.spawn(connect_candidate(candidate, egress.clone()));
                }
            }
            continue;
        }

        match attempts.join_next().await {
            Some(result) => match completed_attempt(Some(result)) {
                Ok(socket) => return Ok(socket),
                Err(error) => last_error = Some(error),
            },
            None => {
                return Err(last_error.unwrap_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotConnected,
                        "no webhook address candidate was connectable",
                    )
                }))
            }
        }
    }
}

fn completed_attempt(
    result: Option<Result<io::Result<TokioSocket>, tokio::task::JoinError>>,
) -> io::Result<TokioSocket> {
    match result.expect("at least one webhook dial attempt is active") {
        Ok(result) => result,
        Err(error) => Err(io::Error::other(format!(
            "webhook dial task failed: {error}"
        ))),
    }
}

async fn connect_candidate(
    candidate: SocketAddr,
    egress: EgressInterfaceControl,
) -> io::Result<TokioSocket> {
    let interface = select_candidate_egress(candidate, &egress)?;
    tokio::time::timeout(
        CANDIDATE_TIMEOUT,
        TokioSocket::connect_addr_on(candidate, interface.as_ref()),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("webhook TCP connect to {candidate} timed out"),
        )
    })?
}

fn select_candidate_egress(
    candidate: SocketAddr,
    egress: &EgressInterfaceControl,
) -> io::Result<Option<EgressInterface>> {
    let selection = egress.select_for_peer(candidate);
    selection.ensure_connectable()?;
    Ok(selection.interface().cloned())
}
