//! Direct reply identity follows the socket and its immutable dial policy.
use super::*;

impl DirectUdpSockets {
    pub(crate) fn isolate_association(&mut self, session_id: u64, association_id: u64) {
        self.isolated_sessions.insert(session_id, association_id);
    }

    pub(crate) async fn send_to_addr(
        &mut self,
        payload: &[u8],
        target: SocketAddr,
        session_id: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<usize, EngineError> {
        // Recheck literal, cached and mapped addresses on every send.
        let target = policy
            .dial_policy
            .normalize_peer(target)
            .map_err(invalid_policy)?;
        self.refresh_if_stale();
        self.services.ensure_direct_policy_current(policy)?;
        let index = self.socket_for(target, session_id, policy).await?;
        self.send_on_socket(index, payload, target, session_id, policy)
            .await
    }

    pub(super) async fn send_on_socket(
        &mut self,
        index: usize,
        payload: &[u8],
        target: SocketAddr,
        session_id: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<usize, EngineError> {
        self.services.ensure_direct_policy_current(policy)?;
        let target = policy
            .dial_policy
            .normalize_peer(target)
            .map_err(invalid_policy)?;
        let entry = &self.sockets[index];
        if entry.binding.egress_generation != self.services.egress_generation() {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "egress topology changed before sending direct UDP packet",
            )));
        }
        let sent = send_direct_udp_packet(&entry.socket, target, payload).await?;
        Ok(self.record_sent_packet(index, target, session_id, policy, sent))
    }

    pub(super) fn record_sent_packet(
        &mut self,
        index: usize,
        target: SocketAddr,
        session_id: u64,
        policy: &DirectUdpPolicy,
        sent: usize,
    ) -> usize {
        // Successful UDP transmission is irreversible. A concurrent reload
        // must not report failure and make the outbound fallback resend data.
        let entry = &mut self.sockets[index];
        if self.services.direct_policy_is_current(policy)
            && entry.binding.egress_generation == self.services.egress_generation()
        {
            let generation = entry.binding.egress_generation;
            entry.replies.register(target, || {
                DirectUdpResponseFlow::new(
                    session_id,
                    self.services.clone(),
                    policy.clone(),
                    generation,
                )
            });
        }
        sent
    }

    pub(super) async fn socket_for(
        &mut self,
        target: SocketAddr,
        session_id: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<usize, EngineError> {
        let scope = self.isolated_sessions.get(&session_id).copied();
        let selection = self
            .services
            .direct_datagram_selection(target, &policy.dial_policy)?;
        if self.generation != selection.generation() {
            self.sockets.clear();
            self.generation = selection.generation();
        }
        // Compare borrowed identities before materializing a new binding. A
        // warm send must not clone the tag/policy/interface just to reuse it.
        let matches_binding = |socket: &DirectUdpSocket| {
            socket.binding.policy == *policy
                && socket.binding.ipv6 == target.is_ipv6()
                && socket.binding.egress_generation == selection.generation()
                && socket.binding.source == selection.dial_source_address()
                && socket.binding.egress.as_ref() == selection.interface()
                && socket.association_id == scope
        };
        // Preserve an existing flow's socket first. Different logical flows
        // may resolve to the same peer, so never overwrite its reply owner.
        if let Some(index) = self.sockets.iter().position(|socket| {
            matches_binding(socket)
                && socket.replies.flow(&target).map(|flow| flow.session_id) == Some(session_id)
        }) {
            return Ok(index);
        }
        // Endpoint-independent reuse remains the normal case for distinct
        // peers. A same-peer collision needs another socket to stay unambiguous.
        if let Some(index) = self
            .sockets
            .iter()
            .position(|socket| matches_binding(socket) && socket.replies.can_admit(&target))
        {
            return Ok(index);
        }
        let socket = self
            .services
            .bind_direct_datagram_socket(
                target,
                if scope.is_some() {
                    None
                } else {
                    self.preferred_port
                },
                &policy.dial_policy,
                &selection,
            )
            .await?;
        // An old bind completion must never populate a new policy or topology.
        self.services.ensure_direct_policy_current(policy)?;
        if selection.generation() != self.services.egress_generation() {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "egress topology changed while binding direct UDP socket",
            )));
        }
        let id = self.next_socket_id;
        self.next_socket_id = self.next_socket_id.checked_add(1).ok_or_else(|| {
            EngineError::Io(std::io::Error::other(
                "direct UDP socket identity exhausted",
            ))
        })?;
        self.sockets.push(DirectUdpSocket {
            id,
            socket,
            binding: DirectUdpSocketBinding {
                policy: policy.clone(),
                ipv6: target.is_ipv6(),
                egress_generation: selection.generation(),
                source: selection.dial_source_address(),
                egress: selection.interface().cloned(),
            },
            selection,
            association_id: scope,
            replies: ownership::DirectUdpReplies::default(),
        });
        let entry = self.sockets.last().expect("just inserted socket");
        log_direct_socket(entry.id, family_name(target.is_ipv6()), &entry.socket);
        Ok(self.sockets.len() - 1)
    }
}
