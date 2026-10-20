//! Response ownership is local to the native socket that actually sent data.
use super::{DirectUdpResponseFlow, DirectUdpSockets, SocketAddr, MAX_DIRECT_UDP_PEERS_PER_SOCKET};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(super) struct DirectUdpReplies {
    flows: HashMap<SocketAddr, DirectUdpResponseFlow>,
    // Do not forget a retired tuple while other owners keep this socket live.
    retired: HashSet<SocketAddr>,
}

impl DirectUdpReplies {
    pub(super) fn flow(&self, peer: &SocketAddr) -> Option<&DirectUdpResponseFlow> {
        self.flows.get(peer)
    }

    pub(super) fn can_admit(&self, peer: &SocketAddr) -> bool {
        !self.retired.contains(peer)
            && !self.flows.contains_key(peer)
            && self.retired.len() + self.flows.len() < MAX_DIRECT_UDP_PEERS_PER_SOCKET
    }

    pub(super) fn register(
        &mut self,
        peer: SocketAddr,
        make_flow: impl FnOnce() -> DirectUdpResponseFlow,
    ) {
        // Repeated sends preserve the exact owner and already-read tokens.
        self.flows.entry(peer).or_insert_with(make_flow);
    }

    pub(super) fn retire_all(&mut self) {
        self.retired
            .extend(self.flows.drain().map(|(peer, _flow)| peer));
    }

    pub(super) fn is_empty(&self) -> bool {
        self.flows.is_empty()
    }

    pub(super) fn retire_session(&mut self, session_id: u64) {
        self.flows.retain(|peer, flow| {
            if flow.session_id == session_id {
                self.retired.insert(*peer);
                false // Dropping the owner revokes every outstanding token.
            } else {
                true
            }
        });
    }
}

impl DirectUdpSockets {
    pub(crate) fn refresh_if_stale(&mut self) {
        let generation = self.services.egress_generation();
        if self.generation != generation {
            self.close_rotated_sockets(|_| true);
            self.generation = generation;
        } else {
            // Keep the policy fence live on every refresh. Socket-owned tables
            // drop their guards without a second global flow-table sweep.
            let services = self.services.clone();
            self.close_rotated_sockets(|socket| {
                !services.direct_policy_is_current(&socket.binding.policy)
            });
        }
        self.reset_receiver_if_empty();
    }

    pub(crate) fn retire_session(&mut self, session_id: u64) {
        self.retire_reply_mappings(session_id);
        self.retire_sockets(|socket| socket.replies.is_empty());
        self.reset_receiver_if_empty();
    }

    /// Logical ownership only. A peer resource keeps its original descriptors
    /// until the actual containing task's joined receipt authorizes close.
    pub(super) fn retire_reply_mappings(&mut self, session_id: u64) {
        // One bounded table traversal per socket, not a socket search per flow.
        for socket in &mut self.sockets {
            socket.replies.retire_session(session_id);
        }
        self.isolated_sessions.remove(&session_id);
    }
}
