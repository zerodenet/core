//! The declared browser listener, its actual accepted entries and original Scope.
//! The private browser protocol still owns HTTP/authentication/WebSocket framing.
use super::{idle::IdleQueue, BrowserDialer, BrowserDialerOptions, Inner};
use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};
use tokio::time::Instant;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_platform_tokio::{
    contract::{
        component::{AcceptedTcpLimits, ResourceAccess, ResourceDeclaration},
        lifecycle::{CloseReport, ResourceLifecycle, TaskClass, Unfinished},
        scope::{TaskId, TaskJoinWitness, TaskRegistrar, WeakTaskRegistrar},
        CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
    },
    AcceptedTcpClaim, AcceptedTcpClosed, AcceptedTcpTaskReceiver,
};
use zero_traits::packet_metadata::PacketMetadataArc;

mod peers;
pub(super) use peers::{Peer, Peers};

/// Cold preparation over the caller's existing ResourceAccess and TaskRegistrar.
/// It neither creates a Host/Scope nor opens an independent listener or pool.
pub struct ScopedBrowserDialer {
    dialer: BrowserDialer,
    address: Destination,
    resources: Arc<dyn ResourceAccess>,
    tasks: WeakTaskRegistrar,
    options: BrowserDialerOptions,
    pub(super) peers: PacketMetadataArc<Peers>,
    driver: Option<(TaskId, TaskJoinWitness)>,
    local: OnceLock<SocketAddr>,
    page_url: OnceLock<String>,
    attempted: bool,
}
impl ScopedBrowserDialer {
    /// Same finite bound is used in the actual resource declaration and driver.
    pub fn declaration(
        address: Destination,
        options: &BrowserDialerOptions,
    ) -> Result<ResourceDeclaration, ChannelError> {
        super::server::validate_options(options)
            .map_err(|source| failure(Stage::Prepare, Reason::InvalidConfig).with_source(source))?;
        match &address.address {
            Address::Ipv4(bytes) if std::net::Ipv4Addr::from(*bytes).is_loopback() => {}
            Address::Ipv6(bytes) if std::net::Ipv6Addr::from(*bytes).is_loopback() => {}
            _ => return Err(failure(Stage::Prepare, Reason::InvalidConfig)),
        }
        let sockets = NonZeroUsize::new(
            options
                .idle_capacity
                .checked_add(8)
                .ok_or_else(|| failure(Stage::Prepare, Reason::Capacity))?,
        )
        .unwrap();
        Ok(ResourceDeclaration::TcpListener {
            destination: address,
            accepted: Some(AcceptedTcpLimits {
                max_sockets: sockets,
                max_tasks_per_socket: NonZeroUsize::new(1).unwrap(),
                max_resources_per_socket: 0,
            }),
            max_bind_attempts: NonZeroUsize::new(1).unwrap(),
        })
    }
    pub fn prepare(
        address: Destination,
        options: BrowserDialerOptions,
        resources: Arc<dyn ResourceAccess>,
        tasks: &TaskRegistrar,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let _ = Self::declaration(address.clone(), &options)?;
        let memory = resources.packet_driver_metadata()?;
        let book = resources
            .carrier_buffer_budget()
            .ok_or_else(|| failure(Stage::Prepare, Reason::Unsupported))?;
        let factory = super::memory::Factory {
            book,
            metadata: memory.clone(),
        };
        let idle = IdleQueue::hosted(options.idle_capacity, &memory)
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let peers = super::paid_arc(
            Peers::new(memory.clone(), options.idle_capacity + 8),
            &memory,
        )
        .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let cancel = cancel.child_token();
        let inner = super::InnerRef::hosted(
            Inner {
                idle,
                cancellation: cancel,
                task_timeout: options.task_timeout,
                max_task_bytes: options.max_task_bytes,
                max_payload_bytes: options.max_payload_bytes,
                factory: Some(factory),
            },
            &memory,
        )
        .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        Ok(Self {
            dialer: BrowserDialer { inner },
            address,
            resources,
            tasks: tasks.downgrade(),
            options,
            peers,
            driver: None,
            local: OnceLock::new(),
            page_url: OnceLock::new(),
            attempted: false,
        })
    }
    pub fn dialer(&self) -> BrowserDialer {
        self.dialer.clone()
    }
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local.get().copied()
    }
    pub fn page_url(&self) -> Option<&str> {
        self.page_url.get().map(String::as_str)
    }
}
impl ResourceLifecycle for ScopedBrowserDialer {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        if self.attempted || self.dialer.inner.cancellation.is_cancelled() {
            return Err(failure(Stage::Prepare, Reason::Revoked));
        }
        self.attempted = true;
        let mut listener = tokio::select! { biased;
            _ = self.dialer.inner.cancellation.cancelled() => return Err(failure(Stage::Prepare, Reason::Cancelled)),
            result = tokio::time::timeout_at(deadline, self.resources.listen_tcp(self.address.clone())) => result
                .map_err(|source| failure(Stage::Prepare, Reason::Timeout).with_source(source))??,
        };
        let local = listener
            .local_addr()
            .map_err(|source| failure(Stage::Prepare, Reason::Io).with_source(source))?;
        let receiver = listener
            .accepted_task_receiver()
            .ok_or_else(|| failure(Stage::Prepare, Reason::Unsupported))?;
        self.peers
            .receiver
            .set(receiver)
            .map_err(|_| failure(Stage::Prepare, Reason::StalePlan))?;
        let factory = self
            .dialer
            .inner
            .factory
            .as_ref()
            .expect("hosted factory was frozen before first IO")
            .clone();
        let registration = super::server::registration_hosted(local, &self.options, &factory)
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let _ = self.page_url.set(registration.page_url.clone());
        let _ = self.local.set(local);
        let idle = self.dialer.inner.idle.clone();
        let cancel = self.dialer.inner.cancellation.clone();
        let peers = self.peers.clone();
        let task = self
            .tasks
            .spawn_tracked(TaskClass::Critical, async move {
                // The actual accept driver also collects returned peer originals.
                // It has no strong Registrar and creates no cleanup side task.
                let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
                poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                'accepting: loop {
                    let preparing = Peers::prepare(&peers, &factory);
                    tokio::pin!(preparing);
                    let prepared = loop {
                        let changed = peers.changed.notified();
                        tokio::pin!(changed);
                        changed.as_mut().enable();
                        tokio::select! { biased;
                            _ = cancel.cancelled() => break 'accepting,
                            _ = &mut changed => { peers.reap()?; },
                            _ = poll.tick() => { peers.reap()?; },
                            result = &mut preparing => break result?,
                        }
                    };
                    let accepted = loop {
                        let changed = peers.changed.notified();
                        tokio::pin!(changed);
                        changed.as_mut().enable();
                        tokio::select! { biased;
                            _ = cancel.cancelled() => break 'accepting,
                            _ = &mut changed => { peers.reap()?; },
                            _ = poll.tick() => { peers.reap()?; },
                            accepted = listener.accept_owned_wait() => break accepted?,
                        }
                    };
                    let (accepted, remote) = accepted;
                    let child = cancel.child_token();
                    let (original, memory) =
                        Peers::install(&peers, accepted.claim(), child.clone(), prepared)?;
                    let idle = idle.clone();
                    let registration = registration.clone();
                    let notification = peers.clone();
                    let issued = original.clone();
                    let spawned = accepted.spawn_handler_bound(
                        TaskClass::Auxiliary,
                        move |stream, tasks| async move {
                            let task = tasks
                                .current_task()
                                .ok_or_else(|| failure(Stage::Prepare, Reason::Revoked))?;
                            issued.0.install_task(task)?;
                            if remote.ip().is_loopback() {
                                super::server::serve(
                                    stream,
                                    idle,
                                    registration,
                                    child,
                                    None,
                                    Some(super::original::BrowserUse(issued)),
                                    memory,
                                )
                                .await;
                            }
                            notification.changed.notify_waiters();
                            Ok(())
                        },
                    );
                    match spawned {
                        Ok(task) => original.0.install_task(task)?,
                        Err(source) => return Err(source),
                    }
                }
                idle.close();
                drop(listener); // only its IO view; Host retains original provider
                Ok(())
            })
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        self.driver = Some(task);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.dialer.close();
    }
    async fn shutdown(&mut self, _deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        let mut report = CloseReport::default();
        if let Some((id, witness)) = &self.driver {
            if witness
                .try_joined()
                .is_none_or(|joined| !joined.registration_released())
            {
                report.unfinished.push(Unfinished::Task(*id));
            }
        }
        if self.peers.receiver.get().is_some() && !self.peers.reap()? {
            report.unfinished.push(Unfinished::Resource(
                "actual browser accepted originals, users or Scope joins remain pending".into(),
            ));
        }
        // Native listener/accepted close reports and TaskScope joins remain in
        // the original Host owner. This report cannot fabricate those receipts.
        Ok(report)
    }
}
impl Drop for ScopedBrowserDialer {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
pub(super) fn failure(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
