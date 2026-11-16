//! Explicit caller budget, sole host provider, and the original peer's driver.
use super::*;
mod inline;
use crate::runtime::{
    inbound_owners::{PeerBoundDatagramAuthority, PeerBoundDatagramRegistration},
    udp_dispatch::UdpDispatch,
    udp_socket::endpoint::{
        dispatch::DirectDispatchEndpoint, peer::DirectBoundPeerOptions, DirectEndpointBinding,
        DirectEndpointIdentity, DirectEndpointLimits, DirectEndpointOwner,
    },
};
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};
use zero_core::{
    component::{NetworkGeneration, SessionId},
    Session,
};
use zero_platform_tokio::contract::{component::ResourceAccess, ChannelError};

/// No defaults: outer peer/task capacity is not this provider's native budget.
#[derive(Clone, Copy)]
pub(crate) struct BoundDirectBudget {
    pub(crate) limits: DirectEndpointLimits,
    pub(crate) max_bindings: NonZeroUsize,
    pub(crate) pre_io_bind_attempts: NonZeroUsize,
    pub(crate) start_deadline: tokio::time::Instant,
}
pub(crate) struct BoundDirectSockets {
    pub(super) bridge: DirectDispatchEndpoint,
    pub(super) isolation: HashMap<u64, u64>,
    pub(super) generation: u64,
    authority: Authority,
    resources: Arc<dyn ResourceAccess>,
    pre_io_bind_attempts: NonZeroUsize,
}
fn channel(error: ChannelError) -> EngineError {
    std::io::Error::other(error).into()
}
enum Authority {
    Peer(Arc<PeerBoundDatagramAuthority>),
    Inline(Box<inline::Original>),
}
impl BoundDirectBudget {
    pub(crate) fn for_accepted(config: &zero_config::RuntimeConfig) -> Self {
        // Existing independent native ceiling and global logical retirement cap.
        // Epoch metadata shares that finite cap; reload may add current policies.
        // Targets do not each consume a policy binding.
        Self {
            limits: DirectEndpointLimits {
                logical: NonZeroUsize::new(4096).unwrap(),
                native_sockets: NonZeroUsize::new(
                    config.runtime.effective_udp_direct_max_sockets(),
                )
                .unwrap(),
                pending_native_opens: NonZeroUsize::MIN,
                pending_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
                candidates_per_job: NonZeroUsize::new(u16::MAX as usize).unwrap(),
            },
            max_bindings: NonZeroUsize::new(4096).unwrap(),
            pre_io_bind_attempts: NonZeroUsize::new(2).unwrap(),
            start_deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        }
    }
}
impl BoundDirectSockets {
    pub(crate) fn registration(&self) -> Option<PeerBoundDatagramRegistration> {
        match &self.authority {
            Authority::Peer(authority) => Some(authority.registration()),
            Authority::Inline(_) => None,
        }
    }
    pub(crate) async fn close_originals(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError> {
        match &mut self.authority {
            Authority::Inline(original) => original.close(deadline).await,
            Authority::Peer(_) => Ok(zero_platform_tokio::contract::lifecycle::CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "bound UDP originals require their real peer owner".into(),
                    ),
                ],
                ..Default::default()
            }),
        }
    }
    pub(super) async fn new(
        runtime: &UdpIngressRuntime,
        budget: BoundDirectBudget,
    ) -> Result<Self, EngineError> {
        if runtime.inline_direct() {
            return inline::build(runtime, budget).map_err(channel);
        }
        let context = runtime.peer_resources().ok_or_else(|| {
            std::io::Error::other("controlled Direct UDP requires its actual peer context")
        })?;
        let slot = runtime.direct_peer_driver().ok_or_else(|| {
            std::io::Error::other("controlled Direct UDP requires its original peer driver")
        })?;
        let execution = runtime.services().packet_path_execution().tcp();
        let queue = execution.config().runtime.network.udp_receive_queue.clone();
        let retirement = execution.retired_datagrams();
        let observer = runtime.services().network().direct_datagram_observer();
        let ((bridge, driver, resources), authority) = context
            .claim_bound_outbound_datagram(
                budget.start_deadline,
                budget.limits.native_sockets,
                budget.max_bindings,
                |grant| {
                    let (owner, driver) = DirectEndpointOwner::for_bound_peer(
                        DirectBoundPeerOptions {
                            identity: DirectEndpointIdentity {
                                resource: grant.identity.instance,
                                ingress: context.ingress(),
                                dispatcher: grant.identity.association,
                            },
                            task: grant.identity.task,
                            queue,
                            limits: budget.limits,
                        },
                        context.binding(),
                        observer,
                    )?;
                    let bridge = DirectDispatchEndpoint::new(owner.endpoint(), retirement);
                    Ok((
                        zero_platform_tokio::contract::lifecycle::erase_lifecycle(owner),
                        (bridge, driver, grant.resources),
                    ))
                },
            )
            .await
            .map_err(channel)?;
        // The exact owner is already installed. Failed submission retains it
        // for actual peer-join/native cleanup; it never falls back to legacy I/O.
        slot.submit(driver).map_err(channel)?;
        Ok(Self {
            bridge,
            resources,
            authority: Authority::Peer(authority),
            isolation: HashMap::new(),
            generation: 0,
            pre_io_bind_attempts: budget.pre_io_bind_attempts,
        })
    }
    async fn refresh_existing(
        &mut self,
        runtime: &UdpIngressRuntime,
        session: &Session,
        policy: &crate::runtime::udp_socket::DirectUdpPolicy,
    ) -> Result<(), EngineError> {
        if self.bridge.binding_is_current(SessionId(session.id)) {
            return Ok(());
        }
        // Reuse the host-selected policy of this live business flow. This is
        // not another route/group pick or a new business admission.
        let selected = crate::inventory::DirectDatagramTarget {
            tag: policy.tag.clone().unwrap_or_else(|| "direct".into()),
            policy_tag: policy.tag.clone(),
            dial_policy: policy.dial_policy.clone(),
            dial_generation: policy.generation,
        };
        self.bridge.retire_session(SessionId(session.id));
        // admit refreshes the same original driver before authorizing a new
        // epoch, consuming the old logical/mapping and native-view retirement.
        self.admit(&selected, runtime, session).await
    }
    async fn admit(
        &mut self,
        operation: &crate::inventory::DirectDatagramTarget,
        runtime: &UdpIngressRuntime,
        session: &Session,
    ) -> Result<(), EngineError> {
        let execution = runtime.services().packet_path_execution().tcp();
        let generation =
            NetworkGeneration(runtime.services().upstream().egress_control().generation());
        let source = match runtime.peer_resources() {
            Some(context) => {
                operation
                    .socket_source(execution.current_source, runtime, session, context)
                    .await
            }
            None => operation.accepted_socket_source(execution.current_source, runtime, session),
        }
        .map_err(channel)?;
        if let Authority::Inline(original) = &mut self.authority {
            original.start().await.map_err(channel)?;
        }
        self.bridge
            .refresh_sources(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
            .await
            .map_err(channel)?;
        let authorized = match &self.authority {
            Authority::Peer(authority) => authority.authorize(source, generation),
            Authority::Inline(original) => original.authorize(source, generation),
        }
        .map_err(channel)?;
        self.bridge
            .admit_bound(
                SessionId(session.id),
                DirectEndpointBinding {
                    resources: self.resources.clone(),
                    id: authorized.id,
                    generation: authorized.generation,
                    policy: authorized.policy,
                    source: authorized.source,
                    pre_io_bind_attempts: self.pre_io_bind_attempts,
                },
                self.isolation.get(&session.id).copied(),
                match &self.authority {
                    Authority::Peer(_) => runtime
                        .peer_resources()
                        .expect("real peer authority")
                        .binding()
                        .cancellation()
                        .child_token(),
                    Authority::Inline(original) => original.cancellation(),
                },
            )
            .map_err(channel)?;
        self.generation = generation.0;
        Ok(())
    }
}
impl UdpDispatch {
    pub(crate) async fn refresh_bound_direct_flow(
        &mut self,
        session: &Session,
        policy: &crate::runtime::udp_socket::DirectUdpPolicy,
    ) -> Result<(), EngineError> {
        match &mut self.direct_socket {
            #[cfg(test)]
            DispatchDirectSockets::Legacy(_) => Ok(()),
            DispatchDirectSockets::Bound(socket) => {
                socket
                    .refresh_existing(&self.runtime.with_current_snapshot(), session, policy)
                    .await
            }
        }
    }
    pub(crate) async fn admit_bound_direct(
        &mut self,
        operation: &crate::inventory::DirectDatagramTarget,
        session: &Session,
    ) -> Result<(), EngineError> {
        match &mut self.direct_socket {
            #[cfg(test)]
            DispatchDirectSockets::Legacy(_) => Ok(()),
            DispatchDirectSockets::Bound(socket) => {
                socket
                    .admit(operation, &self.runtime.with_current_snapshot(), session)
                    .await
            }
        }
    }
    /// Completion of this exact provider, not other gateways in the dispatcher.
    pub(crate) fn bound_direct_registration(&self) -> Option<PeerBoundDatagramRegistration> {
        match &self.direct_socket {
            #[cfg(test)]
            DispatchDirectSockets::Legacy(_) => None,
            DispatchDirectSockets::Bound(socket) => socket.registration(),
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
impl BoundDirectSockets {
    pub(crate) fn original_views_for_test(
        &self,
    ) -> Vec<(
        zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
        zero_platform_tokio::contract::component::BoundDatagramView,
    )> {
        match &self.authority {
            Authority::Inline(original) => original.original_views_for_test(),
            Authority::Peer(_) => panic!("fixture must consume the actual inline provider"),
        }
    }
}
