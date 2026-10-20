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
use pending_bound::PendingNativeBound;
mod readiness;
pub(crate) use observation::consume_reported_error;
pub(super) use pending_bound::pending_preparation_stopped;
pub(super) use readiness::readiness_stopped;
pub(crate) use readiness::InboundReady;
mod accepted;
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
    exit_fault: OnceLock<SharedError>,
}
pub(crate) struct InboundOwner {
    identity: ComponentInstanceId,
    #[cfg(feature = "raw-ip-runtime")]
    scope_task_limit: NonZeroUsize,
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) icmp_probe: OnceLock<crate::component::instance::HostIcmpProbeGrant>,
    pub(super) recovery: OnceLock<Arc<dyn crate::protocol_registry::InboundRecovery>>,
    shutdown: watch::Sender<bool>,
    cancel: CancellationToken,
    scope: AsyncMutex<TaskScope>,
    registrar: TaskRegistrar,
    resources: Mutex<Vec<Arc<Resource>>>,
    report: Mutex<CloseReport>,
    failure: Mutex<Option<SharedError>>,
    accepted_receiver: OnceLock<zero_platform_tokio::AcceptedTcpTaskReceiver>,
    client_failure: OnceLock<zero_platform_tokio::contract::scope::TaskJoined>,
    driver: OnceLock<TaskId>,
    driver_finished: AtomicBool,
    scope_closed: AtomicBool,
    physically_closed: AtomicBool,
    joined_count: Mutex<usize>,
    task_failures: Mutex<std::collections::BTreeMap<TaskId, terminal::InboundTaskFailure>>,
    terminal: OnceLock<Arc<InboundTerminal>>,
    observation: Arc<observation::ObservationDisposition>,
    ready: InboundReady,
    installation_complete: AtomicBool,
    pending_native_bound: Mutex<Option<Arc<PendingNativeBound>>>,
    pending_native_cause: Mutex<Option<SharedError>>,
    #[cfg(feature = "managed-datagram-runtime")]
    peers: Mutex<Vec<Arc<super::InboundPeerTasks>>>,
}
#[derive(Clone)]
pub(crate) struct InboundTaskAccess {
    #[cfg(feature = "managed-datagram-runtime")]
    pub(super) identity: ComponentInstanceId,
    registrar: TaskRegistrar,
    cancel: CancellationToken,
    shutdown: watch::Sender<bool>,
}
impl InboundTaskAccess {
    pub(crate) fn request_stop(&self) {
        self.cancel.cancel();
        self.shutdown.send_replace(true);
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
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
                    ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                        .with_source(source)
                })
            })
    }
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
    pub(super) fn new(
        identity: ComponentInstanceId,
        limit: NonZeroUsize,
        shutdown: watch::Sender<bool>,
    ) -> Self {
        let scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(limit));
        let registrar = scope.registrar();
        Self {
            identity,
            #[cfg(feature = "raw-ip-runtime")]
            scope_task_limit: limit,
            recovery: OnceLock::new(),
            #[cfg(feature = "raw-ip-runtime")]
            icmp_probe: OnceLock::new(),
            shutdown,
            cancel: CancellationToken::new(),
            scope: AsyncMutex::new(scope),
            registrar,
            resources: Mutex::new(Vec::new()),
            report: Mutex::new(CloseReport::default()),
            failure: Mutex::new(None),
            accepted_receiver: OnceLock::new(),
            client_failure: OnceLock::new(),
            driver: OnceLock::new(),
            driver_finished: AtomicBool::new(false),
            scope_closed: AtomicBool::new(false),
            physically_closed: AtomicBool::new(false),
            joined_count: Mutex::new(0),
            task_failures: Mutex::default(),
            terminal: OnceLock::new(),
            observation: Arc::new(observation::ObservationDisposition::new(identity)),
            ready: InboundReady::new(),
            installation_complete: AtomicBool::new(true),
            pending_native_bound: Mutex::new(None),
            pending_native_cause: Mutex::new(None),
            #[cfg(feature = "managed-datagram-runtime")]
            peers: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn install_resource(
        &self,
        identity: ComponentInstanceId,
        owner: Box<dyn DynResourceLifecycle>,
    ) -> Result<(), EngineError> {
        self.install_resource_inner(identity, owner, false)
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
        self.install_resource_inner(identity, owner, true)
    }
    fn install_resource_inner(
        &self,
        identity: ComponentInstanceId,
        owner: Box<dyn DynResourceLifecycle>,
        native_on_driver_exit: bool,
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
            #[cfg(feature = "managed-datagram-runtime")]
            identity: self.identity,
            registrar: self.registrar.clone(),
            cancel: self.cancel.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
    #[cfg(feature = "managed-datagram-runtime")]
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
    #[cfg(feature = "raw-ip-runtime")]
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
        if used > self.scope_task_limit.get() {
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
