use super::*;
use crate::component::instance::{retained, SharedError};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock,
    },
};
use tokio::sync::Mutex as AsyncMutex;
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, TaskFailure},
    scope::{TaskClass, TaskExit, TaskRegistrar, TaskScope},
    ChannelError, CloseReason, Impact, Reason, Stage,
};
#[cfg(feature = "raw-ip-runtime")]
mod native_progress;
mod observation;
#[cfg(feature = "managed-datagram-runtime")]
mod peer_cleanup;
mod pending_bound;
mod publication;
#[cfg(test)]
use pending_bound::PendingNativeBound;
mod readiness;
pub(crate) use observation::{consume_reported_error, preserve_io_kind};
pub(super) use pending_bound::pending_preparation_stopped;
pub(super) use readiness::readiness_stopped;
pub(crate) use readiness::InboundReady;
mod accepted;
mod acquisition;
pub(in crate::runtime) use acquisition::AcquisitionPause;
#[cfg(feature = "managed-datagram-runtime")]
mod continuity;
mod driver;
mod shutdown;
mod terminal;
pub(crate) use terminal::{ClosedInboundReceipt, InboundTerminal};
#[cfg(test)]
mod tests;
struct Resource {
    identity: ComponentInstanceId,
    owner: AsyncMutex<Box<dyn DynResourceLifecycle>>,
    stopped: AtomicBool,
    native_on_driver_exit: bool,
    requires_scope_join: bool,
    exit_fault: OnceLock<SharedError>,
}
pub(crate) struct InboundOwner {
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) packet_pause:
        Arc<OnceLock<Arc<zero_platform_tokio::contract::packet::PacketSourcePause>>>,
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    pub(super) packet_native:
        OnceLock<std::sync::Weak<super::bind::native::udp::NativeDatagramGrant>>,
    #[cfg(test)]
    pub(crate) original_listener_address: Arc<OnceLock<std::net::SocketAddr>>,
    #[cfg(test)]
    last_accepted_join: Mutex<Option<zero_platform_tokio::contract::scope::TaskJoined>>,
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) packet_source:
        Arc<Mutex<Option<crate::runtime::inbound_operation::raw_ip::PacketSourceContext>>>,
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) source_input: OnceLock<[u8; 32]>,
    pub(super) source_dependencies: OnceLock<Arc<[ComponentInstanceId]>>,
    pub(super) inbound_resources: Mutex<
        Option<std::sync::Weak<dyn zero_platform_tokio::contract::component::ResourceAccess>>,
    >,
    #[cfg(feature = "managed-datagram-runtime")]
    continuity: Mutex<Option<Arc<crate::runtime::route_runtime::DatagramContinuityHost>>>,
    identity: ComponentInstanceId,
    #[cfg(feature = "managed-datagram-runtime")]
    scope_task_limit: NonZeroUsize,
    #[cfg(feature = "managed-datagram-runtime")]
    pub(super) shared_scope_children: bool,
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) icmp_probe: OnceLock<crate::component::instance::HostIcmpProbeGrant>,
    pub(super) recovery: OnceLock<Arc<dyn crate::protocol_registry::InboundRecovery>>,
    publication: OnceLock<(
        std::sync::Weak<zero_config::RuntimeConfig>,
        zero_engine::ConfigurationPublication,
    )>,
    shutdown: watch::Sender<bool>,
    cancel: CancellationToken,
    scope: AsyncMutex<TaskScope>,
    registrar: TaskRegistrar,
    resources: Mutex<Vec<Arc<Resource>>>,
    report: Mutex<CloseReport>,
    failure: Mutex<Option<SharedError>>,
    accepted_receiver: Mutex<Vec<zero_platform_tokio::AcceptedTcpTaskReceiver>>,
    acquisition: Arc<acquisition::Acquisition>,
    client_failure: OnceLock<zero_platform_tokio::contract::scope::TaskJoined>,
    driver: OnceLock<TaskId>,
    #[cfg(feature = "raw-ip-runtime")]
    main_registration: Arc<MainRegistration>,
    driver_finished: AtomicBool,
    driver_was_stopping: OnceLock<bool>,
    scope_closed: AtomicBool,
    physically_closed: AtomicBool,
    joined_count: Mutex<usize>,
    task_failures: Mutex<std::collections::BTreeMap<TaskId, terminal::InboundTaskFailure>>,
    terminal: OnceLock<Arc<InboundTerminal>>,
    observation: Arc<observation::ObservationDisposition>,
    ready: InboundReady,
    installation_complete: AtomicBool,
    #[cfg(test)]
    pending_native_bound: Mutex<Option<Arc<PendingNativeBound>>>,
    pending_native_cause: Mutex<Option<SharedError>>,
    #[cfg(feature = "managed-datagram-runtime")]
    peers: Arc<Mutex<Vec<Arc<super::InboundPeerTasks>>>>,
}
#[cfg(feature = "raw-ip-runtime")]
#[derive(Default)]
struct MainRegistration {
    checkpoint: Mutex<Option<zero_platform_tokio::contract::packet::PacketSourceCheckpoint>>,
    witness: OnceLock<zero_platform_tokio::contract::scope::TaskJoinWitness>,
    published: tokio::sync::Notify,
}
#[derive(Clone)]
pub(crate) struct InboundTaskAccess {
    acquisition: Arc<acquisition::Acquisition>,
    #[cfg(feature = "managed-datagram-runtime")]
    peer_groups: std::sync::Weak<Mutex<Vec<Arc<super::InboundPeerTasks>>>>,
    #[cfg(any(feature = "managed-datagram-runtime", feature = "raw-ip-runtime"))]
    pub(super) identity: ComponentInstanceId,
    registrar: TaskRegistrar,
    #[cfg(feature = "raw-ip-runtime")]
    main_registration: std::sync::Weak<MainRegistration>,
    #[cfg(feature = "raw-ip-runtime")]
    packet_pause: Arc<OnceLock<Arc<zero_platform_tokio::contract::packet::PacketSourcePause>>>,
    #[cfg(feature = "raw-ip-runtime")]
    packet_source: std::sync::Weak<
        Mutex<Option<crate::runtime::inbound_operation::raw_ip::PacketSourceContext>>,
    >,
    cancel: CancellationToken,
    shutdown: watch::Sender<bool>,
}
impl InboundTaskAccess {
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime) fn source_instance(&self) -> ComponentInstanceId {
        self.identity
    }

    /// Observe the exact registered source main, distinct from its native wire.
    /// The main may run before spawn_tracked returns on another runtime worker;
    /// wait for publication of that original witness rather than fabricate one.
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn source_main_witness(
        &self,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        let failure = || ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component);
        let original = self.main_registration.upgrade().ok_or_else(failure)?;
        loop {
            let published = original.published.notified();
            tokio::pin!(published);
            published.as_mut().enable();
            if self.cancel.is_cancelled() {
                return Err(failure());
            }
            if let Some(witness) = original.witness.get() {
                if witness.is_joined()
                    || !witness.scope_identity().resource_open()
                    || !witness
                        .scope_identity()
                        .same_scope(&self.registrar.scope_identity())
                {
                    return Err(failure());
                }
                return Ok(witness.clone());
            }
            tokio::select! { biased;
                _ = self.cancel.cancelled() => return Err(failure()),
                _ = published => {},
            }
        }
    }

    /// Observe only this Source's existing pause cell, after its actual main
    /// registration has been published. This grants no actor/native authority.
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn packet_source_admission(
        &self,
    ) -> Result<Arc<zero_platform_tokio::contract::packet::PacketSourcePause>, ChannelError> {
        let main = self.source_main_witness().await?;
        let pause = self
            .packet_pause
            .get()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?;
        if pause.instance() != self.identity || !pause.scope().same_scope(&main.scope_identity()) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        Ok(pause.clone())
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime) fn has_packet_source(&self) -> bool {
        self.packet_source
            .upgrade()
            .is_some_and(|source| source.lock().unwrap().is_some())
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime) fn packet_source(
        &self,
    ) -> Option<crate::runtime::inbound_operation::raw_ip::PacketSourceContext> {
        self.packet_source.upgrade()?.lock().unwrap().take()
    }

    #[cfg(feature = "managed-datagram-runtime")]
    pub(in crate::runtime) fn peer_registrar(
        &self,
    ) -> zero_platform_tokio::contract::scope::WeakTaskRegistrar {
        self.registrar.downgrade()
    }
    pub(crate) fn request_stop(&self) {
        self.cancel.cancel();
        self.shutdown.send_replace(true);
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
    #[cfg(test)]
    pub(crate) fn spawn(
        &self,
        work: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<TaskId, EngineError> {
        self.spawn_critical(work)
            .map_err(|source| std::io::Error::other(source).into())
    }
    pub(super) fn spawn_critical_tracked(
        &self,
        work: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<
        (
            TaskId,
            zero_platform_tokio::contract::scope::TaskJoinWitness,
        ),
        zero_platform_tokio::contract::scope::SpawnError,
    > {
        self.registrar
            .spawn_tracked(TaskClass::Critical, async move {
                work.await.map_err(|source| {
                    #[cfg(test)]
                    eprintln!("actual Source tracked Critical work returned original error: {source:#?}");
                    ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                        .with_source(source)
                })
            })
    }
    #[cfg(test)]
    pub(super) fn spawn_critical(
        &self,
        work: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<TaskId, zero_platform_tokio::contract::scope::SpawnError> {
        self.registrar.spawn(TaskClass::Critical, async move {
            work.await.map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(source)
            })
        })
    }
}
impl InboundOwner {
    /// Stop this same Source synchronously while its existing registry retains
    /// all native/Scope cleanup. This request issues no retirement receipt.
    pub(crate) fn request_stop(&self) {
        self.stop();
    }

    pub(super) fn new(
        identity: ComponentInstanceId,
        limit: NonZeroUsize,
        shutdown: watch::Sender<bool>,
    ) -> Self {
        let scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(limit));
        let registrar = scope.registrar();
        Self {
            #[cfg(feature = "raw-ip-runtime")]
            packet_pause: Arc::default(),
            #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
            packet_native: OnceLock::new(),
            #[cfg(feature = "raw-ip-runtime")]
            source_input: OnceLock::new(),
            source_dependencies: OnceLock::new(),
            inbound_resources: Mutex::new(None),
            #[cfg(feature = "raw-ip-runtime")]
            packet_source: Arc::new(Mutex::new(None)),
            #[cfg(feature = "managed-datagram-runtime")]
            continuity: Mutex::new(None),
            identity,
            #[cfg(feature = "managed-datagram-runtime")]
            scope_task_limit: limit,
            #[cfg(feature = "managed-datagram-runtime")]
            shared_scope_children: false,
            recovery: OnceLock::new(),
            publication: OnceLock::new(),
            #[cfg(feature = "raw-ip-runtime")]
            icmp_probe: OnceLock::new(),
            shutdown,
            cancel: CancellationToken::new(),
            scope: AsyncMutex::new(scope),
            registrar,
            resources: Mutex::new(Vec::new()),
            report: Mutex::new(CloseReport::default()),
            failure: Mutex::new(None),
            accepted_receiver: Mutex::new(Vec::new()),
            acquisition: Arc::default(),
            client_failure: OnceLock::new(),
            driver: OnceLock::new(),
            #[cfg(feature = "raw-ip-runtime")]
            main_registration: Arc::default(),
            driver_finished: AtomicBool::new(false),
            driver_was_stopping: OnceLock::new(),
            scope_closed: AtomicBool::new(false),
            physically_closed: AtomicBool::new(false),
            joined_count: Mutex::new(0),
            task_failures: Mutex::default(),
            terminal: OnceLock::new(),
            observation: Arc::new(observation::ObservationDisposition::new(identity)),
            #[cfg(test)]
            original_listener_address: Arc::new(OnceLock::new()),
            #[cfg(test)]
            last_accepted_join: Mutex::new(None),
            ready: InboundReady::new(),
            installation_complete: AtomicBool::new(true),
            #[cfg(test)]
            pending_native_bound: Mutex::new(None),
            pending_native_cause: Mutex::new(None),
            #[cfg(feature = "managed-datagram-runtime")]
            peers: Arc::default(),
        }
    }

    pub(super) fn install_resource(
        &self,
        identity: ComponentInstanceId,
        owner: Box<dyn DynResourceLifecycle>,
    ) -> Result<(), EngineError> {
        self.install_resource_inner(identity, owner, false, false)
    }
    pub(super) fn install_listener_native(
        &self,
        identity: ComponentInstanceId,
        owner: Box<dyn DynResourceLifecycle>,
    ) -> Result<(), EngineError> {
        if identity != self.identity {
            return Err(
                std::io::Error::other("listener native identity differs from its owner").into(),
            );
        }
        self.install_resource_inner(identity, owner, true, false)
    }
    /// The original native provider requires actual joins from this Scope.
    /// It stays installed before acquire, but this Scope's own main task cannot
    /// consume those joins. The external retained owner drives the close.
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) fn install_after_scope_resource(
        &self,
        identity: ComponentInstanceId,
        owner: Box<dyn DynResourceLifecycle>,
    ) -> Result<(), EngineError> {
        self.install_resource_inner(identity, owner, false, true)
    }
    fn install_resource_inner(
        &self,
        identity: ComponentInstanceId,
        owner: Box<dyn DynResourceLifecycle>,
        native_on_driver_exit: bool,
        requires_scope_join: bool,
    ) -> Result<(), EngineError> {
        let mut resources = self.resources.lock().expect("inbound resources poisoned");
        if self.cancel.is_cancelled() {
            return Err(std::io::Error::other("inbound resource installation is closed").into());
        }
        resources.push(Arc::new(Resource {
            identity,
            owner: AsyncMutex::new(owner),
            stopped: AtomicBool::new(false),
            native_on_driver_exit,
            requires_scope_join,
            exit_fault: OnceLock::new(),
        }));
        Ok(())
    }
    #[cfg(all(test, feature = "managed-datagram-runtime"))]
    pub(crate) fn peer_tasks_for_test(&self) -> Arc<super::InboundPeerTasks> {
        self.peers
            .lock()
            .expect("peer owners poisoned")
            .first()
            .expect("actual peer group installed")
            .clone()
    }
    pub(crate) fn is_running(&self) -> bool {
        !self.cancel.is_cancelled()
            && self.driver.get().is_some()
            && !self.driver_finished.load(Ordering::Acquire)
    }
    pub(crate) fn task_access(&self) -> InboundTaskAccess {
        InboundTaskAccess {
            acquisition: self.acquisition.clone(),
            #[cfg(feature = "managed-datagram-runtime")]
            peer_groups: Arc::downgrade(&self.peers),
            #[cfg(any(feature = "managed-datagram-runtime", feature = "raw-ip-runtime"))]
            identity: self.identity,
            registrar: self.registrar.clone(),
            #[cfg(feature = "raw-ip-runtime")]
            main_registration: Arc::downgrade(&self.main_registration),
            #[cfg(feature = "raw-ip-runtime")]
            packet_pause: self.packet_pause.clone(),
            #[cfg(feature = "raw-ip-runtime")]
            packet_source: Arc::downgrade(&self.packet_source),
            cancel: self.cancel.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
    #[cfg(feature = "managed-datagram-runtime")]
    #[cfg(test)]
    pub(super) fn install_peer_tasks(
        &self,
        protocols: ProtocolInventory,
    ) -> Arc<super::InboundPeerTasks> {
        let group = Arc::new(super::InboundPeerTasks::new(
            self.identity,
            self.cancel.clone(),
            protocols,
        ));
        let mut peers = self.peers.lock().expect("peer owners poisoned");
        assert!(
            !self.cancel.is_cancelled(),
            "peer installation after inbound shutdown"
        );
        peers.push(group.clone());
        group
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(super) fn install_child_group(
        &self,
        protocols: ProtocolInventory,
        limit: NonZeroUsize,
    ) -> Result<Arc<super::InboundPeerTasks>, EngineError> {
        let mut groups = self.peers.lock().expect("peer owners poisoned");
        if self.cancel.is_cancelled() || self.driver.get().is_some() {
            return Err(std::io::Error::other(
                "child groups must be installed before the actual driver starts",
            )
            .into());
        }
        let used = groups
            .iter()
            .try_fold(1_usize, |sum, group| {
                sum.checked_add(group.declared_limit())
            })
            .and_then(|sum| sum.checked_add(limit.get()))
            .ok_or_else(|| std::io::Error::other("actual child task declaration exhausted"))?;
        // System TUN's optional total is enforced by this same actual
        // registrar at each spawn, before any native acquisition. Potential
        // per-group ceilings are not occupied worker registrations. Other
        // listeners retain their existing declaration reservation policy.
        if !self.shared_scope_children && used > self.scope_task_limit.get() {
            return Err(std::io::Error::other(
                "actual child groups exceed the listener's declared task scope",
            )
            .into());
        }
        let group = Arc::new(super::InboundPeerTasks::with_limit(
            self.identity,
            self.cancel.clone(),
            protocols,
            limit,
        ));
        groups.push(group.clone());
        Ok(group)
    }
    fn publish_driver(&self, task: TaskId) {
        let report = self.report.lock().expect("inbound close report poisoned");
        self.driver.set(task).expect("one actual listener driver");
        self.ready.wake();
        // A concurrent shutdown may have already joined the real registered
        // task. Publish only that observed receipt, never an invented exit.
        if report.exited_tasks.contains(&task) {
            self.driver_finished.store(true, Ordering::Release);
        }
    }
    fn stop(&self) {
        self.ready.wake();
        self.cancel.cancel();
        self.shutdown.send_replace(true);
        #[cfg(feature = "managed-datagram-runtime")]
        for peers in self.peers.lock().expect("peer owners poisoned").iter() {
            peers.begin_shutdown();
        }
    }
    fn result(&self) -> Result<(), EngineError> {
        match self
            .failure
            .lock()
            .expect("inbound failure poisoned")
            .as_ref()
        {
            Some(error) => Err(std::io::Error::other(retained(error.clone())).into()),
            None => Ok(()),
        }
    }
}

#[cfg(feature = "raw-ip-runtime")]
impl InboundOwner {
    pub(in crate::runtime) fn has_packet_source_pause(&self) -> bool {
        self.packet_pause.get().is_some()
    }
    /// The original typed Packet bind grant is installed before its first
    /// native operation. Its weak pointer expiring is not a NoNative proof.
    /// This is conservative replacement metadata, never a readiness receipt.
    pub(in crate::runtime) fn has_packet_source_custody(&self) -> bool {
        #[cfg(feature = "datagram-route-runtime")]
        if self.packet_native.get().is_some() {
            return true;
        }
        self.has_packet_source_pause()
    }
    pub(in crate::runtime) fn prepare_packet_source_pause(
        &self,
    ) -> Arc<zero_platform_tokio::contract::packet::PacketSourcePause> {
        self.packet_pause
            .get_or_init(|| {
                zero_platform_tokio::contract::packet::PacketSourcePause::new(
                    self.identity,
                    self.registrar.scope_identity(),
                    self.cancel.clone(),
                )
            })
            .clone()
    }
}
#[cfg(feature = "raw-ip-runtime")]
impl InboundTaskAccess {
    pub(in crate::runtime) async fn take_packet_main_checkpoint(
        &self,
    ) -> Result<zero_platform_tokio::contract::packet::PacketSourceCheckpoint, ChannelError> {
        let actual = self.source_main_witness().await?;
        let main = self
            .main_registration
            .upgrade()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?;
        let checkpoint = main.checkpoint.lock().unwrap().take().ok_or_else(|| {
            ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component)
        })?;
        if !checkpoint.witness().same_registration(&actual) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        Ok(checkpoint)
    }
}
