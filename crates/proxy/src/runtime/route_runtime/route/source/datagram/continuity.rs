//! Reconnectable Datagram dispatch belongs to the real listener, not a carrier.
mod actor;
mod carrier;
mod owner;
use super::*;
use crate::runtime::{
    inbound_owners::{HostPeerBinding, InboundPeerTasks, InboundTaskAccess},
    mux_udp::{MuxUdpContinuityAttach, MuxUdpContinuityRegistry, MuxUdpContinuityScope},
};
use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use zero_platform_tokio::contract::{inbound::DatagramContinuity, lifecycle::ResourceLifecycle};

pub(in crate::runtime) struct Host {
    tasks: InboundTaskAccess,
    group: Arc<InboundPeerTasks>,
    limit: std::num::NonZeroUsize,
    max_tasks: std::num::NonZeroUsize,
    max_resources: usize,
    registry: MuxUdpContinuityRegistry<Arc<Association>>,
}
impl Host {
    pub(in crate::runtime) fn new(
        tasks: InboundTaskAccess,
        group: Arc<InboundPeerTasks>,
        limit: std::num::NonZeroUsize,
        max_tasks: std::num::NonZeroUsize,
        max_resources: usize,
    ) -> Self {
        Self {
            tasks,
            group,
            limit,
            max_tasks,
            max_resources,
            registry: Default::default(),
        }
    }
    async fn attach(
        self: &Arc<Self>,
        route: InboundRouteRuntime,
        continuity: DatagramContinuity,
        protocol: &str,
        auth: Option<&SessionAuth>,
    ) -> Result<Arc<Association>, EngineError> {
        let services = route.tcp_runtime.runtime_services();
        let ingress = services
            .execution()
            .ingress
            .as_ref()
            .ok_or_else(|| std::io::Error::other("missing association ingress"))?
            .role;
        let scope = MuxUdpContinuityScope::new(
            route.inbound_tag(),
            protocol,
            auth.and_then(|auth| auth.principal_key.as_deref()),
            continuity.key,
        );
        let retention = route.udp_runtime().services().udp_upstream_idle_timeout();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if self.tasks.cancellation().is_cancelled() {
                return Err(std::io::Error::other("listener continuity owner stopped").into());
            }
            let attached = self
                .registry
                .attach_bounded(scope.clone(), retention, self.limit.get())
                .map_err(|_| {
                    std::io::Error::other("declared Datagram association capacity exhausted")
                })?;
            match attached {
                MuxUdpContinuityAttach::Conflict { .. }
                    if tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await
                }
                MuxUdpContinuityAttach::Conflict { .. } => {
                    return Err(std::io::Error::other(
                        "Datagram continuity still attached or retiring",
                    )
                    .into())
                }
                MuxUdpContinuityAttach::Reattached {
                    generation,
                    dispatch: Some(association),
                } => {
                    association.generation.store(generation, Ordering::Release);
                    if association.closing.load(Ordering::Acquire) {
                        return Err(std::io::Error::other(
                            "original Datagram association cleanup remains pending",
                        )
                        .into());
                    }
                    self.registry
                        .publish(&scope, generation, association.clone())
                        .map_err(|_| {
                            std::io::Error::other("Datagram handover lost original identity")
                        })?;
                    return Ok(association);
                }
                MuxUdpContinuityAttach::New { generation } => {
                    let (sender, receiver) = mpsc::channel(1);
                    let association = Arc::new(Association {
                        sender,
                        generation: AtomicU64::new(generation),
                        closing: AtomicBool::new(false),
                        scope: scope.clone(),
                        retention,
                    });
                    let host = self.clone();
                    let original = association.clone();
                    let group = self.group.clone();
                    let accepted = self.group.spawn_bound(&self.tasks, move |binding| {
                        let resources = group.resource_context(binding.clone(), ingress);
                        async move {
                            actor::start(
                                host, original, binding, resources, route, receiver, retention,
                            )
                            .await
                        }
                    });
                    match accepted {
                        Ok(true) => {}
                        denied => {
                            // No task was registered, so no native preparation
                            // could run. Release only this unstarted reservation.
                            self.registry.finish(&scope, generation);
                            return Err(match denied {
                                Err(error) => error,
                                Ok(false) => std::io::Error::other(
                                    "actual listener Scope denied association",
                                )
                                .into(),
                                Ok(true) => unreachable!(),
                            });
                        }
                    }
                    self.registry
                        .publish(&scope, generation, association.clone())
                        .map_err(|_| {
                            std::io::Error::other("association publication lost original")
                        })?;
                    return Ok(association);
                }
                MuxUdpContinuityAttach::Reattached { dispatch: None, .. } => {
                    return Err(std::io::Error::other(
                        "association original owner was not published",
                    )
                    .into())
                }
            }
        }
    }
}
struct Association {
    sender: mpsc::Sender<Attachment>,
    generation: AtomicU64,
    closing: AtomicBool,
    scope: MuxUdpContinuityScope,
    retention: Duration,
}
struct Attachment {
    tx: std::pin::Pin<Box<zero_platform_tokio::contract::inbound::DynInboundDatagramTx>>,
    rx: std::pin::Pin<
        Box<
            dyn zero_platform_tokio::contract::datagram::DatagramRx<
                zero_platform_tokio::contract::inbound::InboundDatagramEvent,
            >,
        >,
    >,
    limits: zero_platform_tokio::contract::datagram::DatagramLimits,
    negotiation: zero_platform_tokio::contract::inbound::InboundDatagramTraffic,
    state: Arc<AsyncMutex<super::owner::State>>,
    continuity: DatagramContinuity,
    auth: Option<SessionAuth>,
    source: Option<std::net::SocketAddr>,
    cancel: CancellationToken,
    done: oneshot::Sender<Result<(), EngineError>>,
}
pub(super) async fn run(
    route: InboundRouteRuntime,
    request: Box<dyn AcceptedDatagram>,
    continuity: DatagramContinuity,
    auth: Option<SessionAuth>,
    cancel: CancellationToken,
) -> Result<(), EngineError> {
    let host = route.continuity.clone().ok_or_else(|| {
        std::io::Error::other("standard Datagram continuity has no actual listener owner")
    })?;
    let protocol = request.facts().protocol.as_str().to_owned();
    let association = match host
        .attach(route.clone(), continuity.clone(), &protocol, auth.as_ref())
        .await
    {
        Ok(association) => association,
        Err(error) => {
            let _ = request.respond(ReplyOutcome::UpstreamFailed).await;
            return Err(error);
        }
    };
    let reservation = Reservation {
        host,
        association: association.clone(),
        generation: association.generation.load(Ordering::Acquire),
        submitted: false,
    };
    carrier::run(route, request, continuity, auth, cancel, reservation).await
}
struct Reservation {
    host: Arc<Host>,
    association: Arc<Association>,
    generation: u64,
    submitted: bool,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.submitted && !self.association.closing.load(Ordering::Acquire) {
            let _ = self.host.registry.detach(
                &self.association.scope,
                self.generation,
                self.association.retention,
                Some(self.association.clone()),
            );
        }
    }
}

impl Host {
    pub(in crate::runtime) fn group(
        &self,
    ) -> Arc<crate::runtime::inbound_owners::InboundPeerTasks> {
        self.group.clone()
    }
}
