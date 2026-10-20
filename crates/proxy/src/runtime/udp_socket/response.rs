//! Revocable ownership of replies already read from a Direct socket.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use super::{DirectUdpPolicy, SocketAddr};
use crate::protocol_registry::UdpNetworkServices;

#[derive(Clone)]
pub(crate) struct DirectUdpResponseSource {
    pub(crate) sender: SocketAddr,
    pub(crate) session_id: Option<u64>,
    // None is used only by the protocol-owned local peer response path.
    pub(crate) guard: Option<DirectUdpResponseGuard>,
}

impl DirectUdpResponseSource {
    pub(crate) fn is_current(&self) -> bool {
        self.guard
            .as_ref()
            .is_none_or(DirectUdpResponseGuard::is_current)
    }
}

#[derive(Clone)]
pub(crate) struct DirectUdpResponseGuard {
    state: Arc<DirectUdpResponseState>,
    logical_cancel: Option<zero_platform_tokio::contract::CancellationToken>,
}

struct DirectUdpResponseState {
    active: AtomicBool,
    services: UdpNetworkServices,
    policy: DirectUdpPolicy,
    egress_generation: u64,
}

impl DirectUdpResponseGuard {
    pub(crate) fn is_current(&self) -> bool {
        self.state.active.load(Ordering::Acquire)
            && self
                .state
                .services
                .direct_policy_is_current(&self.state.policy)
            && self.state.egress_generation == self.state.services.egress_generation()
            && !self
                .logical_cancel
                .as_ref()
                .is_some_and(|cancel| cancel.is_cancelled())
    }
    pub(super) fn with_logical_cancel(
        mut self,
        cancel: zero_platform_tokio::contract::CancellationToken,
    ) -> Self {
        // The native source token is unchanged. Add this actual association's
        // cancellation fence without allocating/replacing its socket identity.
        self.logical_cancel = Some(cancel);
        self
    }
}

pub(super) struct DirectUdpResponseFlow {
    pub(super) session_id: u64,
    pub(super) guard: DirectUdpResponseGuard,
}

impl DirectUdpResponseFlow {
    pub(super) fn new(
        session_id: u64,
        services: UdpNetworkServices,
        policy: DirectUdpPolicy,
        egress_generation: u64,
    ) -> Self {
        Self {
            session_id,
            guard: DirectUdpResponseGuard {
                state: Arc::new(DirectUdpResponseState {
                    active: AtomicBool::new(true),
                    services,
                    policy,
                    egress_generation,
                }),
                logical_cancel: None,
            },
        }
    }
}

impl Drop for DirectUdpResponseFlow {
    fn drop(&mut self) {
        // Removing an exact reply owner revokes every copy of its read token.
        // A new socket, even at the same local port, gets a distinct token.
        self.guard.state.active.store(false, Ordering::Release);
    }
}
