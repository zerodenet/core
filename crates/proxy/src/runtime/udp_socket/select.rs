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
        self.services.ensure_direct_policy_current(policy)?;
        let mut candidates = policy
            .dial_policy
            .filter_candidates(candidates.iter().copied())
            .map_err(invalid_policy)?;
        let mut last_error = None;
        while let Some(target) = select_stable_udp_target(logical_target, &candidates, true, true) {
            self.services.ensure_direct_policy_current(policy)?;
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
