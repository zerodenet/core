//! Two original route responsibilities in the same System TUN Source Scope.
mod memory;
mod monitor;
pub(in crate::inbound::tun) use memory::{copy_addresses, copy_key, copy_prefixes};
mod native;
mod worker;
use super::*;
pub(in crate::inbound::tun) use native::Snapshot;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};
use tokio::sync::Notify;
use zero_platform_tokio::contract::{
    lifecycle::{
        erase_lifecycle, CloseReport, DynResourceLifecycle, ResourceLifecycle, Unfinished,
    },
    scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};
use zero_tun::packet_memory::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};
type SourceError = Arc<io::Error>;
#[derive(Clone)]
pub(in crate::inbound::tun) struct OwnedRoutes(zero_tun::packet_memory::PacketMetadataArc<Shared>);
struct Shared {
    proxy: Proxy,
    metadata: Arc<crate::inbound::tun::source::SystemTunSourceState>,
    native: Mutex<native::Native>,
    observed: Mutex<Observed>,
    changed: Notify,
    cancel: CancellationToken,
    closing: AtomicBool,
    jobs: mpsc::Sender<Job>,
    received: Mutex<Option<mpsc::Receiver<Job>>>,
    monitor_io: tokio::sync::Mutex<RouteChangeMonitor>,
    worker: OnceLock<TaskJoinWitness>,
    monitor: OnceLock<TaskJoinWitness>,
}
#[derive(Default)]
struct Observed {
    prepared: Option<Result<super::super::PreparedTunNetwork, SourceError>>,
    ready: Option<Result<Snapshot, SourceError>>,
    physically_closed: bool,
}
enum Job {
    Discover(
        Arc<zero_config::RuntimeConfig>,
        Option<oneshot::Sender<Result<super::super::PreparedTunNetwork, SourceError>>>,
    ),
    Install(String, oneshot::Sender<Result<Snapshot, SourceError>>),
    Reconcile(
        Arc<zero_config::RuntimeConfig>,
        oneshot::Sender<Result<Snapshot, SourceError>>,
    ),
    Close,
}
impl OwnedRoutes {
    pub(in crate::inbound::tun) fn resource(&self) -> Box<dyn DynResourceLifecycle> {
        erase_lifecycle(self.clone())
    }
    /// The actual resource is installed before this call. Both witnesses enter
    /// its persistent state before the first discovery/native job is submitted.
    pub(in crate::inbound::tun) fn prepare(
        &self,
        tasks: &TaskRegistrar,
        config: Arc<zero_config::RuntimeConfig>,
        recovery: mpsc::Receiver<oneshot::Sender<Result<(), String>>>,
    ) -> Result<(), ChannelError> {
        if self.0.worker.get().is_some() || self.0.monitor.get().is_some() {
            return Err(error(
                Reason::StalePlan,
                "same route responsibilities already attempted",
            ));
        }
        let original = self.0.clone();
        let (_, witness) = tasks
            .spawn_blocking_tracked(TaskClass::Critical, move || worker::run(original))
            .map_err(|source| {
                error(Reason::Capacity, "same Source cannot register route worker")
                    .with_source(source)
            })?;
        self.0
            .worker
            .set(witness)
            .map_err(|_| error(Reason::StalePlan, "route worker witness already installed"))?;
        let original = self.0.clone();
        let (_, witness) = tasks
            .spawn_tracked(TaskClass::Critical, monitor::run(original, recovery))
            .map_err(|source| {
                error(
                    Reason::Capacity,
                    "same Source cannot register route monitor",
                )
                .with_source(source)
            })?;
        self.0
            .monitor
            .set(witness)
            .map_err(|_| error(Reason::StalePlan, "route monitor witness already installed"))?;
        self.0
            .jobs
            .try_send(Job::Discover(config, None))
            .map_err(|source| {
                error(Reason::Capacity, "original route job lane is not empty").with_source(
                    match source {
                        mpsc::error::TrySendError::Full(_) => mpsc::error::TrySendError::Full(()),
                        mpsc::error::TrySendError::Closed(_) => {
                            mpsc::error::TrySendError::Closed(())
                        }
                    },
                )
            })
    }
    pub(in crate::inbound::tun) async fn refresh_network(
        &self,
        config: Arc<zero_config::RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<super::super::PreparedTunNetwork, ChannelError> {
        let (completed, result) = oneshot::channel();
        tokio::time::timeout_at(deadline, async {
            self.0
                .jobs
                .send(Job::Discover(config, Some(completed)))
                .await
                .map_err(|source| {
                    error(Reason::Revoked, "original route worker stopped").with_source({
                        let mpsc::error::SendError(_) = source;
                        mpsc::error::SendError(())
                    })
                })?;
            result
                .await
                .map_err(|source| {
                    error(Reason::Io, "original route discovery did not return").with_source(source)
                })?
                .map_err(|source| {
                    error(Reason::Io, "original route discovery failed")
                        .with_source(SharedError(source))
                })
        })
        .await
        .map_err(|source| {
            error(Reason::Timeout, "original route discovery remains pending").with_source(source)
        })?
    }
    pub(in crate::inbound::tun) async fn prepared(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<super::super::PreparedTunNetwork, ChannelError> {
        self.wait(deadline, |observed| observed.prepared.clone())
            .await
    }
    pub(in crate::inbound::tun) async fn ready(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Snapshot, ChannelError> {
        self.wait(deadline, |observed| observed.ready.clone()).await
    }
    async fn wait<T>(
        &self,
        deadline: tokio::time::Instant,
        get: impl Fn(&Observed) -> Option<Result<T, SourceError>>,
    ) -> Result<T, ChannelError> {
        tokio::time::timeout_at(deadline, async {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed); changed.as_mut().enable();
                if let Some(result) = get(&self.0.observed.lock().unwrap_or_else(|error| error.into_inner())) {
                    return result.map_err(|source| error(Reason::Io, "original route operation failed").with_source(SharedError(source)));
                }
                tokio::select! { biased;
                    _ = self.0.cancel.cancelled() => return Err(error(Reason::Revoked,"original route operation was stopped")),
                    _ = changed => {},
                }
            }
        }).await.map_err(|source| error(Reason::Timeout, "original route operation remains pending").with_source(source))?
    }
}
impl ResourceLifecycle for OwnedRoutes {
    async fn start(&mut self, _: tokio::time::Instant) -> Result<(), ChannelError> {
        if self.0.worker.get().is_none() || self.0.monitor.get().is_none() {
            return Err(error(
                Reason::StalePlan,
                "concrete route responsibilities were not registered",
            ));
        }
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.0.closing.store(true, Ordering::Release);
        self.0.cancel.cancel();
        let _ = self.0.jobs.try_send(Job::Close);
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        if self.0.worker.get().is_none() {
            // No SDK job is submitted until its real worker witness is retained.
            // Close the same genuinely unused originals; the host still owes
            // any accepted Scope joins independently of this zero-native state.
            self.0.monitor_io.lock().await.close().map_err(|source| {
                error(Reason::Io, "unused route monitor close failed").with_source(source)
            })?;
            self.0
                .native
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .close()
                .map_err(|source| {
                    error(Reason::Io, "unused route originals remain owned").with_source(source)
                })?;
            self.0
                .observed
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .physically_closed = true;
        }
        for witness in [&self.0.monitor, &self.0.worker]
            .into_iter()
            .filter_map(OnceLock::get)
        {
            if tokio::time::timeout_at(deadline, witness.wait())
                .await
                .is_err()
            {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(
                        "same System TUN route responsibilities have not joined".into(),
                    )],
                    ..Default::default()
                });
            }
        }
        if self
            .0
            .observed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .physically_closed
        {
            Ok(CloseReport::default())
        } else {
            Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "same System TUN native route cleanup remains unconfirmed".into(),
                )],
                ..Default::default()
            })
        }
    }
}
fn error(reason: Reason, message: &str) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(io::Error::other(message.to_owned()))
}
#[derive(Debug)]
struct SharedError(SourceError);
impl std::fmt::Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(test)]
#[path = "owned/tests/metadata.rs"]
mod metadata_tests;
