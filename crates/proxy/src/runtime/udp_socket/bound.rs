//! Host-issued multi-target bindings. This mode has no Engine or legacy opener.
use super::*;
use std::sync::Arc;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{
    component::{
        BoundDatagramRequest, DatagramBindingAuthorization, DatagramBindingId, ResourceAccess,
    },
    ChannelError, DeliveryState, Impact, Reason, Stage,
};
/// Created by the actual resolved host Direct operation, never a protocol policy.
#[derive(Clone)]
pub(crate) struct DirectEndpointBinding {
    pub(crate) resources: Arc<dyn ResourceAccess>,
    pub(crate) id: DatagramBindingId,
    pub(crate) generation: NetworkGeneration,
    pub(crate) policy: DirectUdpPolicy,
    pub(crate) source: Arc<dyn DatagramBindingAuthorization>,
}
impl DirectEndpointBinding {
    pub(crate) fn check_current(&self) -> Result<(), ChannelError> {
        if self.source.policy() != &self.policy.dial_policy {
            let mut failure =
                ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component);
            failure.delivery = DeliveryState::NotSubmitted;
            return Err(failure);
        }
        self.source.check_current(self.generation)
    }
    fn request(&self, peer: SocketAddr) -> BoundDatagramRequest {
        BoundDatagramRequest {
            binding: self.id.clone(),
            peer,
            expected_network_generation: self.generation,
        }
    }
}
impl DirectUdpSockets {
    pub(super) fn new_bound(
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        native_limit: std::num::NonZeroUsize,
    ) -> Self {
        Self {
            sockets: Vec::new(),
            services: None,
            bound: None,
            bound_observer: observer,
            bound_native_limit: Some(native_limit),
            preferred_port: None,
            generation: 0,
            next_socket_id: 0,
            isolated_sessions: HashMap::new(),
            receive: tokio::sync::Mutex::new(receive::DirectUdpReceiver::default()),
            close: close::NativeCloseLedger::default(),
            native_owner: None,
            defer_native_close: true,
            outbound_grant: None,
        }
    }
    pub(super) fn ensure_policy_current(
        &self,
        policy: &DirectUdpPolicy,
    ) -> Result<(), EngineError> {
        if let Some(services) = &self.services {
            return services.ensure_direct_policy_current(policy);
        }
        let Some(bound) = &self.bound else {
            return Err(denied(Reason::InvalidConfig));
        };
        if bound.policy != *policy {
            return Err(denied(Reason::StalePlan));
        }
        bound
            .check_current()
            .map_err(|source| std::io::Error::other(source).into())
    }
    pub(super) fn policy_is_current(&self, policy: &DirectUdpPolicy, generation: u64) -> bool {
        if let Some(services) = &self.services {
            return services.direct_policy_is_current(policy)
                && services.egress_generation() == generation;
        }
        self.bound.as_ref().is_some_and(|bound| {
            bound.policy == *policy
                && bound.generation.0 == generation
                && bound.check_current().is_ok()
        })
    }
    pub(super) fn bound_socket_for(
        &mut self,
        target: SocketAddr,
        session: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<usize, EngineError> {
        let bound = self
            .bound
            .as_ref()
            .ok_or_else(|| denied(Reason::InvalidConfig))?;
        self.ensure_policy_current(policy)?;
        let request = bound.request(target);
        let selection = bound
            .resources
            .select_bound_outbound_datagram(request.clone())
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        if selection.generation() != bound.generation.0 {
            return Err(denied(Reason::StalePlan));
        }
        let isolation = self.isolated_sessions.get(&session).copied();
        let matches = |entry: &DirectUdpSocket| {
            !entry.retired
                && entry.socket.bound_id() == Some(&bound.id)
                && entry.binding.policy == *policy
                && entry.binding.ipv6 == target.is_ipv6()
                && entry.binding.source == selection.dial_source_address()
                && entry.binding.egress.as_ref() == selection.interface()
                && entry.binding.egress_generation == selection.generation()
                && entry.association_id == isolation
        };
        if let Some(index) = self.sockets.iter().position(|entry| {
            matches(entry)
                && entry.replies.flow(&target).map(|flow| flow.session_id) == Some(session)
        }) {
            return Ok(index);
        }
        if let Some(index) = self
            .sockets
            .iter()
            .position(|entry| matches(entry) && entry.replies.can_admit(&target))
        {
            return Ok(index);
        }
        let resources = bound.resources.clone();
        let expected_id = bound.id.clone();
        self.ensure_native_admission()?;
        if self
            .sockets
            .len()
            .checked_add(self.close.bound_occupied())
            .is_none_or(|used| used >= self.bound_native_limit.expect("bound mode limit").get())
        {
            return Err(denied(Reason::Capacity));
        }
        let id = self.next_socket_id;
        self.next_socket_id = id.checked_add(1).ok_or_else(|| denied(Reason::Capacity))?;
        self.sockets
            .try_reserve(1)
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        let binding = DirectUdpSocketBinding {
            policy: policy.clone(),
            ipv6: target.is_ipv6(),
            egress_generation: selection.generation(),
            source: selection.dial_source_address(),
            egress: selection.interface().cloned(),
        };
        let replies = ownership::DirectUdpReplies::default();
        let granted = resources
            .open_bound_outbound_datagram(request)
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        // The provider already holds the original. Install the unique view before
        // any external fence/callback; failed postchecks physically retire it.
        self.sockets.push(DirectUdpSocket {
            id,
            retired: false,
            socket: native::DirectDatagramIo::Bound(granted.socket),
            binding,
            selection: granted.selection,
            association_id: isolation,
            replies,
        });
        self.sockets
            .last_mut()
            .expect("installed bound view")
            .socket
            .set_observer(self.bound_observer.clone())?;
        let entry = self.sockets.last().expect("installed bound view");
        let valid = entry.socket.bound_id() == Some(&expected_id)
            && entry.selection.generation() == entry.binding.egress_generation
            && entry.selection.dial_source_address() == entry.binding.source
            && entry.selection.interface() == entry.binding.egress.as_ref();
        let current = self.ensure_policy_current(policy);
        if !valid || current.is_err() {
            self.close_rotated_sockets(|entry| entry.id == id);
            current?;
            return Err(denied(Reason::StalePlan));
        }
        Ok(self.sockets.len() - 1)
    }
}
fn denied(reason: Reason) -> EngineError {
    let mut failure = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    failure.delivery = DeliveryState::NotSubmitted;
    std::io::Error::other(failure).into()
}
