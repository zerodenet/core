//! Destination selection opens only a permitted family and never retries data.
use super::*;
mod bound;

impl DirectUdpSockets {
    pub(crate) async fn send_new_packet(
        &mut self,
        logical_target: &Address,
        candidates: &[SocketAddr],
        payload: &[u8],
        session_id: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<DirectUdpSentPacket, EngineError> {
        self.refresh_if_stale();
        self.ensure_policy_current(policy)?;
        let candidates = policy
            .dial_policy
            .filter_candidates(candidates.iter().copied())
            .map_err(invalid_policy)?;
        self.send_candidates(logical_target, candidates, payload, session_id, policy)
            .await
    }

    /// Consume the already budgeted candidate allocation from the typed driver.
    /// Filtering in place preserves the original normalization/order semantics.
    pub(super) async fn send_owned_packet(
        &mut self,
        logical_target: &Address,
        mut candidates: Vec<SocketAddr>,
        payload: &[u8],
        session_id: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<DirectUdpSentPacket, OwnedSendFailure> {
        self.refresh_if_stale();
        self.ensure_policy_current(policy)?;
        policy.dial_policy.validate().map_err(invalid_policy)?;
        candidates.retain_mut(
            |candidate| match policy.dial_policy.normalize_peer(*candidate) {
                Ok(normalized) => {
                    *candidate = normalized;
                    true
                }
                Err(_) => false,
            },
        );
        if self.services.is_none() {
            return self
                .send_bound_candidates(logical_target, candidates, payload, session_id, policy)
                .await;
        }
        self.send_candidates(logical_target, candidates, payload, session_id, policy)
            .await
            .map_err(OwnedSendFailure::from)
    }

    async fn send_candidates(
        &mut self,
        logical_target: &Address,
        mut candidates: Vec<SocketAddr>,
        payload: &[u8],
        session_id: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<DirectUdpSentPacket, EngineError> {
        if self.services.is_none() {
            return self
                .send_bound_candidates(logical_target, candidates, payload, session_id, policy)
                .await
                .map_err(OwnedSendFailure::into_error);
        }
        let mut last_error = None;
        while let Some(target) = select_stable_udp_target(logical_target, &candidates, true, true) {
            self.ensure_policy_current(policy)?;
            match self.socket_for(target, session_id, policy).await {
                Ok(index) => {
                    let sent = self
                        .send_on_socket(index, payload, target, session_id, policy)
                        .await?;
                    let socket = &self.sockets[index];
                    return Ok(DirectUdpSentPacket {
                        sent,
                        target,
                        local: socket.socket.local_addr().ok(),
                        selection: socket.selection.clone(),
                    });
                }
                Err(error) => {
                    // Auto may use another family when the first family cannot
                    // bind. The same source/interface policy survives the retry,
                    // and payload is only sent once after successful binding.
                    last_error = Some(error);
                    candidates.retain(|candidate| candidate.is_ipv6() != target.is_ipv6());
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "no direct UDP target satisfies the dial policy",
            ))
        }))
    }
}

/// The sole driver receives settled acquisition evidence outside diagnostics.
pub(super) struct OwnedSendFailure {
    error: EngineError,
    settled: Option<(
        zero_platform_tokio::contract::component::BoundDatagramRetryReceipt,
        zero_platform_tokio::contract::component::BoundDatagramRequest,
    )>,
}
impl OwnedSendFailure {
    fn acquisition(
        error: EngineError,
        receipt: Option<zero_platform_tokio::contract::component::BoundDatagramRetryReceipt>,
        request: zero_platform_tokio::contract::component::BoundDatagramRequest,
    ) -> Self {
        Self {
            error,
            settled: receipt
                .filter(|receipt| receipt.matches(&request))
                .map(|receipt| (receipt, request)),
        }
    }
    pub(in crate::runtime::udp_socket) fn settled_acquisition(&self) -> bool {
        self.settled
            .as_ref()
            .is_some_and(|(receipt, request)| receipt.matches(request))
    }
    pub(in crate::runtime::udp_socket) fn into_error(self) -> EngineError {
        self.error
    }
}
impl From<EngineError> for OwnedSendFailure {
    fn from(error: EngineError) -> Self {
        Self {
            error,
            settled: None,
        }
    }
}
