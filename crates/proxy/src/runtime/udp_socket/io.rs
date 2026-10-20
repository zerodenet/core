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
        if self.outbound_grant.is_some() {
            return self.granted_socket_for(target, session_id, policy);
        }
        let scope = self.isolated_sessions.get(&session_id).copied();
        let selection = self
            .services
            .direct_datagram_selection(target, &policy.dial_policy)?;
        if self.generation != selection.generation() {
            self.close_rotated_sockets(|_| true);
            self.generation = selection.generation();
        }
        // Compare borrowed identities before materializing a new binding. A
        // warm send must not clone the tag/policy/interface just to reuse it.
        let matches_binding = |socket: &DirectUdpSocket| {
            !socket.retired
                && socket.binding.policy == *policy
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
        // Existing healthy mappings above may keep running, but a new native
        // endpoint must not grow an unresolved original-descriptor ledger.
        self.ensure_native_admission()?;
        let id = self.next_socket_id;
        self.next_socket_id = self.next_socket_id.checked_add(1).ok_or_else(|| {
            EngineError::Io(std::io::Error::other(
                "direct UDP socket identity exhausted",
            ))
        })?;
        // Prepare allocations and observation handles before a native fd exists.
        // The external boundary callback still runs exactly once after a
        // successful bind, after the original fd enters this same state.
        self.sockets
            .try_reserve(1)
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        let observer = self.services.direct_datagram_observer();
        let binding = DirectUdpSocketBinding {
            policy: policy.clone(),
            ipv6: target.is_ipv6(),
            egress_generation: selection.generation(),
            source: selection.dial_source_address(),
            egress: selection.interface().cloned(),
        };
        let replies = ownership::DirectUdpReplies::default();
        let preferred = if scope.is_some() {
            None
        } else {
            self.preferred_port
        };
        let socket = if let Some(owner) = &mut self.native_owner {
            let acquired = owner.acquire(target, preferred, &policy.dial_policy, &selection);
            self.close.collect_controlled(owner.take_report());
            native::DirectDatagramIo::Controlled(
                acquired.map_err(|source| EngineError::Io(std::io::Error::other(source)))?,
            )
        } else {
            native::DirectDatagramIo::Raw(
                self.services
                    .bind_direct_datagram_socket(target, preferred, &policy.dial_policy, &selection)
                    .await?,
            )
        };
        self.sockets.push(DirectUdpSocket {
            id,
            retired: false,
            socket,
            binding,
            selection,
            association_id: scope,
            replies,
        });
        // In-place attachment cannot drop the installed socket if the external
        // observer panics. The owner/scope cleanup sees this exact descriptor.
        self.sockets
            .last_mut()
            .expect("just installed socket")
            .socket
            .set_observer(observer)?;
        // A stale completion still owns a real original socket. Retire it into
        // this same ledger before returning the original admission error.
        if let Err(error) = self.services.ensure_direct_policy_current(policy) {
            self.close_rotated_sockets(|socket| socket.id == id);
            return Err(error);
        }
        if self
            .sockets
            .last()
            .expect("just installed socket")
            .binding
            .egress_generation
            != self.services.egress_generation()
        {
            self.close_rotated_sockets(|socket| socket.id == id);
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "egress topology changed while binding direct UDP socket",
            )));
        }
        let entry = self.sockets.last().expect("just inserted socket");
        log_direct_socket(entry.id, family_name(target.is_ipv6()), &entry.socket);
        Ok(self.sockets.len() - 1)
    }
}
