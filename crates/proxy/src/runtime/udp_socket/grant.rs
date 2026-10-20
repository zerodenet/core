//! Exact-peer host grant. The original socket is installed before callbacks.
use super::*;
use std::sync::Arc;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{
    component::{OutboundDatagramRequest, ResourceAccess},
    ChannelError, DeliveryState, Impact, Reason, Stage,
};
pub(super) struct DirectOutboundGrant {
    pub(super) resources: Arc<dyn ResourceAccess>,
    pub(super) peer: SocketAddr,
    pub(super) generation: NetworkGeneration,
}
fn denied(reason: Reason) -> EngineError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    error.delivery = DeliveryState::NotSubmitted;
    std::io::Error::other(error).into()
}
impl DirectUdpSockets {
    pub(super) fn granted_socket_for(
        &mut self,
        target: SocketAddr,
        session: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<usize, EngineError> {
        let grant = self.outbound_grant.as_ref().expect("checked grant mode");
        if target != grant.peer || policy.dial_policy != zero_traits::DialPolicy::default() {
            return Err(denied(Reason::Unsupported));
        }
        if self.services.egress_generation() != grant.generation.0 {
            return Err(denied(Reason::StalePlan));
        }
        let isolation = self.isolated_sessions.get(&session).copied();
        if let Some(index) = self.sockets.iter().position(|entry| {
            !entry.retired
                && entry.binding.policy == *policy
                && entry.binding.egress_generation == grant.generation.0
                && entry.association_id == isolation
                && (entry.replies.flow(&target).map(|flow| flow.session_id) == Some(session)
                    || entry.replies.can_admit(&target))
        }) {
            return Ok(index);
        }
        let resources = grant.resources.clone();
        let request = OutboundDatagramRequest {
            peer: target,
            expected_network_generation: grant.generation,
        };
        self.ensure_native_admission()?;
        let id = self.next_socket_id;
        self.next_socket_id = id.checked_add(1).ok_or_else(|| denied(Reason::Capacity))?;
        self.sockets
            .try_reserve(1)
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        let observer = self.services.direct_datagram_observer();
        let binding = DirectUdpSocketBinding {
            policy: policy.clone(),
            ipv6: target.is_ipv6(),
            egress_generation: request.expected_network_generation.0,
            source: None,
            egress: None,
        };
        let replies = ownership::DirectUdpReplies::default();
        let granted = resources
            .open_outbound_datagram(request)
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        // No external call, allocation, await or postvalidation may intervene:
        // the host moved this exact original descriptor into our persistent owner.
        self.sockets.push(DirectUdpSocket {
            id,
            retired: false,
            socket: native::DirectDatagramIo::Raw(granted.socket),
            selection: granted.selection,
            binding,
            association_id: isolation,
            replies,
        });
        let entry = self.sockets.last_mut().expect("just installed original");
        entry.binding.source = entry.selection.dial_source_address();
        entry.binding.egress = entry.selection.interface().cloned();
        entry.socket.set_observer(observer)?;
        let current = self.services.ensure_direct_policy_current(policy);
        let valid_generation = entry.selection.generation()
            == request.expected_network_generation.0
            && self.services.egress_generation() == request.expected_network_generation.0;
        if current.is_err() || !valid_generation {
            self.retire_sockets(|entry| entry.id == id);
            current?;
            return Err(denied(Reason::StalePlan));
        }
        let entry = self.sockets.last().expect("original retained");
        log_direct_socket(id, family_name(target.is_ipv6()), &entry.socket);
        Ok(self.sockets.len() - 1)
    }
}
