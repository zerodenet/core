//! Same native writer; only a sealed settled acquisition can select another family.
use super::*;
use std::error::Error;

#[derive(Debug)]
struct BindFailures {
    previous: EngineError,
    final_error: EngineError,
}
impl std::fmt::Display for BindFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "prior native bind failed: {}; final operation failed: {}",
            self.previous, self.final_error
        )
    }
}
impl Error for BindFailures {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.final_error)
    }
}
fn with_previous(previous: Option<EngineError>, error: EngineError) -> EngineError {
    match previous {
        None => error,
        Some(previous) => std::io::Error::other(BindFailures {
            previous,
            final_error: error,
        })
        .into(),
    }
}
fn invalid_grant(message: &'static str) -> EngineError {
    use zero_platform_tokio::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};
    let mut error = ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
        .with_source(std::io::Error::other(message));
    error.delivery = DeliveryState::NotSubmitted;
    std::io::Error::other(error).into()
}
impl DirectUdpSockets {
    pub(in super::super) async fn send_bound_candidates(
        &mut self,
        logical: &Address,
        mut candidates: Vec<SocketAddr>,
        payload: &[u8],
        session: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<DirectUdpSentPacket, OwnedSendFailure> {
        let limit = self
            .bound
            .as_ref()
            .ok_or_else(|| invalid_grant("missing actual bound Direct grant"))?
            .pre_io_bind_attempts
            .get();
        let mut attempts = 0_usize;
        // The old algorithm removes the failed family: at most one saved
        // prior failure and one final outcome, never a lifetime error history.
        let mut previous = None;
        while let Some(target) = select_stable_udp_target(logical, &candidates, true, true) {
            if let Err(error) = self.ensure_policy_current(policy) {
                return Err(with_previous(previous, error).into());
            }
            attempts = attempts
                .checked_add(1)
                .ok_or_else(|| invalid_grant("bound UDP attempt counter exhausted"))?;
            let request = self
                .bound
                .as_ref()
                .expect("same original grant")
                .request(target);
            match self.bound_socket_attempt(target, session, policy) {
                Ok(index) => {
                    // Never retry send errors or repeat submitted bytes. Only
                    // socket acquisition, before this first send, can fall back.
                    let sent = self
                        .send_on_socket(index, payload, target, session, policy)
                        .await
                        .map_err(|error| with_previous(previous, error))?;
                    let socket = &self.sockets[index];
                    return Ok(DirectUdpSentPacket {
                        sent,
                        target,
                        local: socket.socket.local_addr().ok(),
                        selection: socket.selection.clone(),
                    });
                }
                Err(failure) => {
                    let safe = failure
                        .retry
                        .as_ref()
                        .is_some_and(|receipt| receipt.matches(&request));
                    let another_family = candidates
                        .iter()
                        .any(|candidate| candidate.is_ipv6() != target.is_ipv6());
                    if !safe || attempts >= limit || !another_family {
                        return Err(OwnedSendFailure::acquisition(
                            with_previous(previous, failure.error),
                            failure.retry,
                            request,
                        ));
                    }
                    // No source downcast, delivery guess or error-kind policy.
                    // The next iteration rechecks current/cancel/generation and
                    // requests another grant from the same persistent provider.
                    previous = Some(failure.error);
                    candidates.retain(|candidate| candidate.is_ipv6() != target.is_ipv6());
                }
            }
        }
        Err(previous
            .unwrap_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "no direct UDP target satisfies the dial policy",
                )
                .into()
            })
            .into())
    }
}
