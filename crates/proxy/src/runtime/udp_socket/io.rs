//! Direct reply identity follows the socket and its immutable dial policy.
use super::*;

impl DirectUdpSockets {
    pub(crate) fn isolate_association(&mut self, session_id: u64, association_id: u64) {
        self.isolated_sessions.insert(session_id, association_id);
    }

    pub(crate) fn retire_session(&mut self, session_id: u64) {
        for ((socket_id, peer), owner) in &self.response_flows {
            if owner.session_id == session_id {
                if let Some(socket) = self
                    .sockets
                    .iter_mut()
                    .find(|socket| socket.id == *socket_id)
                {
                    socket.retired_peers.insert(*peer);
                }
            }
        }
        self.response_flows
            .retain(|_, flow| flow.session_id != session_id);
        self.isolated_sessions.remove(&session_id);
        // Retain every unaffected live mapping, but do not keep tombstones or
        // socket descriptors once there is nobody left to receive a reply.
        self.sockets
            .retain(|socket| self.response_flows.keys().any(|(id, _)| *id == socket.id));
        self.reset_receiver_if_empty();
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
        let entry = &self.sockets[index];
        if self.services.direct_policy_is_current(policy)
            && entry.binding.egress_generation == self.services.egress_generation()
        {
            self.response_flows
                .entry((entry.id, target))
                .or_insert_with(|| {
                    DirectUdpResponseFlow::new(
                        session_id,
                        self.services.clone(),
                        policy.clone(),
                        entry.binding.egress_generation,
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
            self.response_flows.clear();
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
                && self
                    .response_flows
                    .get(&(socket.id, target))
                    .map(|flow| flow.session_id)
                    == Some(session_id)
        }) {
            return Ok(index);
        }
        // Endpoint-independent reuse remains the normal case for distinct
        // peers. A same-peer collision needs another socket to stay unambiguous.
        if let Some(index) = self.sockets.iter().position(|socket| {
            matches_binding(socket)
                && !socket.retired_peers.contains(&target)
                && !self.response_flows.contains_key(&(socket.id, target))
                && socket.retired_peers.len()
                    + self
                        .response_flows
                        .keys()
                        .filter(|(id, _)| *id == socket.id)
                        .count()
                    < MAX_DIRECT_UDP_PEERS_PER_SOCKET
        }) {
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
        log_direct_socket(family_name(target.is_ipv6()), &socket);
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
            retired_peers: HashSet::new(),
        });
        Ok(self.sockets.len() - 1)
    }
}
