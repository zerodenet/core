//! Structured peer driver; the real host peer owns its registration and join.
mod close;
pub(crate) mod job;
use super::*;
use crate::runtime::inbound_owners::{HostPeerBinding, JoinedPeer};
pub(crate) use job::DirectPeerDriverSlot;
#[cfg(test)]
use std::net::SocketAddr;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
#[cfg(test)]
use zero_core::component::NetworkGeneration;
use zero_core::component::TaskId;
#[cfg(test)]
use zero_platform_tokio::contract::component::ResourceAccess;
use zero_platform_tokio::contract::lifecycle::ComponentError;

#[cfg(test)]
pub(crate) struct DirectPeerOptions {
    pub(crate) identity: DirectEndpointIdentity,
    pub(crate) task: TaskId,
    pub(crate) target: SocketAddr,
    pub(crate) network_generation: NetworkGeneration,
    pub(crate) preferred_port: Option<u16>,
    pub(crate) queue: UdpReceiveQueueConfig,
    pub(crate) limits: DirectEndpointLimits,
    pub(crate) resources: Arc<dyn ResourceAccess>,
}
/// Explicit caller grants only. The host provider owns the actual native cap.
pub(crate) struct DirectBoundPeerOptions {
    pub(crate) identity: DirectEndpointIdentity,
    pub(crate) task: TaskId,
    pub(crate) queue: UdpReceiveQueueConfig,
    pub(crate) limits: DirectEndpointLimits,
}
pub(super) struct PeerOwner {
    pub(super) binding: Arc<HostPeerBinding>,
    task: TaskId,
    // Retained proof, never reconstructed from driver Ready, Drop or cancel.
    joined: Option<Arc<JoinedPeer>>,
}
/// Poll this future in the same actual registered peer task as its consumer.
/// It never creates a scope, registers a task or fabricates a task receipt.
pub(crate) struct DirectPeerDriver {
    stop: CancellationToken,
    future: Pin<Box<dyn Future<Output = Result<(), ComponentError>> + Send>>,
}
impl Future for DirectPeerDriver {
    type Output = Result<(), ComponentError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
impl DirectEndpointOwner {
    #[cfg(test)]
    pub(crate) fn for_peer(
        options: DirectPeerOptions,
        services: UdpNetworkServices,
        binding: Arc<HostPeerBinding>,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<(Self, DirectPeerDriver), ChannelError> {
        let DirectPeerOptions {
            identity,
            task,
            target,
            network_generation,
            preferred_port,
            queue,
            limits,
            resources,
        } = options;
        if preferred_port.is_some()
            || limits.logical.get() != 1
            || limits.native_sockets.get() != 1
            || limits.pending_native_opens.get() != 1
            || limits.candidates_per_job.get() != 1
        {
            return Err(error(
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        }
        if services.egress_generation() != network_generation.0 {
            return Err(error(
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let mut sockets = DirectUdpSockets::new(services, None);
        sockets.defer_native_close = true;
        sockets.outbound_grant = Some(super::super::grant::DirectOutboundGrant {
            resources,
            peer: target,
            generation: network_generation,
        });
        Self::build_peer(identity, task, sockets, queue, limits, binding, observer)
    }
    pub(crate) fn for_bound_peer(
        options: DirectBoundPeerOptions,
        binding: Arc<HostPeerBinding>,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<(Self, DirectPeerDriver), ChannelError> {
        let DirectBoundPeerOptions {
            identity,
            task,
            queue,
            limits,
        } = options;
        let sockets = DirectUdpSockets::new_bound(observer.clone(), limits.native_sockets);
        Self::build_peer(identity, task, sockets, queue, limits, binding, observer)
    }
    fn build_peer(
        identity: DirectEndpointIdentity,
        task: TaskId,
        sockets: DirectUdpSockets,
        queue: UdpReceiveQueueConfig,
        limits: DirectEndpointLimits,
        binding: Arc<HostPeerBinding>,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<(Self, DirectPeerDriver), ChannelError> {
        let (shared, commands) = shared(
            identity,
            sockets,
            queue,
            limits,
            binding.cancellation(),
            observer,
        )?;
        let driver_shared = shared.clone();
        let driver_binding = binding.clone();
        let driver = DirectPeerDriver {
            stop: shared.scope_cancel.clone(),
            future: Box::pin(async move {
                // A registrar may poll the actual peer before spawn_bound publishes
                // its TaskId. Neither native I/O nor driver startup precedes binding.
                let actual = driver_binding.wait_bound().await?;
                if actual != task {
                    return Err(error(
                        Stage::Prepare,
                        Reason::StalePlan,
                        DeliveryState::NotSubmitted,
                    ));
                }
                loop {
                    let changed = driver_shared.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    {
                        let state = driver_shared
                            .state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if state.closing || driver_shared.scope_cancel.is_cancelled() {
                            return Ok(());
                        }
                        if state.started {
                            break;
                        }
                    }
                    tokio::select! {
                        _ = changed => {},
                        _ = driver_shared.scope_cancel.cancelled() => return Ok(()),
                    }
                }
                driver::run(driver_shared, commands).await
            }),
        };
        Ok((
            Self {
                shared,
                scope: None,
                accepted: None,
                peer: Some(PeerOwner {
                    binding,
                    task,
                    joined: None,
                }),
                commands: None,
                report: Default::default(),
                io_report: Default::default(),
                failures: Vec::new(),
            },
            driver,
        ))
    }
}

impl DirectEndpointOwner {
    pub(crate) fn for_bound_accepted(
        options: DirectBoundPeerOptions,
        caller: zero_platform_tokio::AcceptedCaller,
        cancel: CancellationToken,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<(Self, DirectPeerDriver), ChannelError> {
        caller.check_open()?;
        let witness = caller
            .task_join_witness()
            .filter(|witness| witness.task_id() == Some(options.task))
            .ok_or_else(|| {
                error(
                    Stage::Prepare,
                    Reason::StalePlan,
                    DeliveryState::NotSubmitted,
                )
            })?;
        let sockets = DirectUdpSockets::new_bound(observer.clone(), options.limits.native_sockets);
        let (shared, commands) = shared(
            options.identity,
            sockets,
            options.queue,
            options.limits,
            cancel,
            observer,
        )?;
        let original = shared.clone();
        let driver = DirectPeerDriver {
            stop: shared.scope_cancel.clone(),
            future: Box::pin(async move {
                caller.check_open()?;
                loop {
                    let changed = original.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    {
                        let state = original
                            .state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if state.closing || original.scope_cancel.is_cancelled() {
                            return Ok(());
                        }
                        if state.started {
                            break;
                        }
                    }
                    tokio::select! {_=changed=>{},_=original.scope_cancel.cancelled()=>return Ok(())}
                }
                driver::run(original, commands).await
            }),
        };
        Ok((
            Self {
                shared,
                scope: None,
                peer: None,
                accepted: Some(witness),
                commands: None,
                report: Default::default(),
                io_report: Default::default(),
                failures: Vec::new(),
            },
            driver,
        ))
    }
}
