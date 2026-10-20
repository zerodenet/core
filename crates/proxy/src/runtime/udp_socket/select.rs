//! Destination selection opens only a permitted family and never retries data.
use super::*;

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
    ) -> Result<DirectUdpSentPacket, EngineError> {
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
        self.send_candidates(logical_target, candidates, payload, session_id, policy)
            .await
    }

    async fn send_candidates(
        &mut self,
        logical_target: &Address,
        mut candidates: Vec<SocketAddr>,
        payload: &[u8],
        session_id: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<DirectUdpSentPacket, EngineError> {
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
                    if self.services.is_none() {
                        // The scoped provider returned the actual authorization
                        // or acquisition failure. It currently grants no replay;
                        // never route around that boundary through another family.
                        return Err(error);
                    }
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
