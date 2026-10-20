//! Explicit caller budget, sole host provider, and the original peer's driver.
use super::*;
use crate::runtime::{
    inbound_owners::{PeerBoundDatagramAuthority, PeerBoundDatagramRegistration},
    udp_dispatch::{operation::DirectUdpFlowOperation, UdpDispatch},
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
    pub(crate) start_deadline: tokio::time::Instant,
}
pub(crate) struct BoundDirectSockets {
    pub(super) bridge: DirectDispatchEndpoint,
    pub(super) isolation: HashMap<u64, u64>,
    pub(super) generation: u64,
    authority: Arc<PeerBoundDatagramAuthority>,
    resources: Arc<dyn ResourceAccess>,
}
fn channel(error: ChannelError) -> EngineError {
    std::io::Error::other(error).into()
}
impl BoundDirectSockets {
    pub(super) async fn new(
        runtime: &UdpIngressRuntime,
        budget: BoundDirectBudget,
    ) -> Result<Self, EngineError> {
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
            authority,
            isolation: HashMap::new(),
            generation: 0,
        })
    }
    async fn admit(
        &mut self,
        operation: &DirectUdpFlowOperation,
        runtime: &UdpIngressRuntime,
        session: &Session,
    ) -> Result<(), EngineError> {
        let context = runtime
            .peer_resources()
            .ok_or_else(|| std::io::Error::other("actual Direct peer context missing"))?;
        let execution = runtime.services().packet_path_execution().tcp();
        let generation =
            NetworkGeneration(runtime.services().upstream().egress_control().generation());
        let source = operation
            .socket_source(execution.current_source, runtime, session, context)
            .await
            .map_err(channel)?;
        let authorized = self
            .authority
            .authorize(source, generation)
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
                },
                self.isolation.get(&session.id).copied(),
                context.binding().cancellation().child_token(),
            )
            .map_err(channel)?;
        self.generation = generation.0;
        Ok(())
    }
}
impl UdpDispatch {
    pub(crate) async fn admit_bound_direct(
        &mut self,
        operation: &DirectUdpFlowOperation,
        session: &Session,
    ) -> Result<(), EngineError> {
        match &mut self.direct_socket {
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
            DispatchDirectSockets::Legacy(_) => None,
            DispatchDirectSockets::Bound(socket) => Some(socket.authority.registration()),
        }
    }
}

#[cfg(test)]
mod tests;
