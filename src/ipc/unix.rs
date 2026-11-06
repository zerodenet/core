use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};
use zero_platform_tokio::contract::CancellationToken;
use zero_platform_tokio::contract::{lifecycle::ChannelControl, CloseReason};
use zero_platform_tokio::{
    ControlledTcpControl, ControlledUnixListener, ControlledUnixListenerOwner,
};
use zero_proxy::ProxyHandle;

use super::connection;

/// Default control socket path relative to $HOME.
pub const DEFAULT_SOCKET_REL_PATH: &str = ".zero/control.sock";

pub struct IpcServerHandle {
    shutdown: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    task: crate::ipc::owner::ControlTask,
    listener: ControlledUnixListenerOwner,
    originals: Arc<UnixOriginals>,
    path: PathBuf,
    started: std::sync::atomic::AtomicBool,
    retired: std::sync::atomic::AtomicBool,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
}

impl IpcServerHandle {
    pub(crate) fn retirement_complete(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }
    pub(crate) async fn wait_terminated(&self) -> io::Result<()> {
        self.task.join().await
    }
    pub async fn shutdown(&self) -> io::Result<()> {
        self.task.close_admission();
        if let Some(shutdown) = self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = shutdown.send(());
        }
        let result = self.task.join().await;
        let accepted = self.originals.close_all().await;
        let native = match self.listener.close(super::owner::close_deadline()).await {
            Ok(closed) if self.listener.matches_closed(&closed) => {
                self.originals
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                self.listener.remove_bound_path(&closed)
            }
            Ok(_) => Err(io::Error::other("IPC listener closed a foreign original")),
            Err(error) => Err(error),
        };
        if accepted.is_ok() && native.is_ok() {
            self.retired.store(true, Ordering::Release);
        }
        super::owner::finish_work([result, accepted, native])
    }
}

/// Compute the default socket path: `$HOME/.zero/control.sock`.
pub fn default_socket_path() -> Option<PathBuf> {
    let home = dirs_home()?;
    Some(home.join(DEFAULT_SOCKET_REL_PATH))
}

/// Resolve the socket path in order of precedence:
///
/// 1. Explicit path from CLI `--control-socket`.
/// 2. `{exe_dir}/control.sock` — sibling to the executable.
/// 3. `$HOME/.zero/control.sock` — global fallback.
pub fn resolve_socket_path(explicit: Option<&str>) -> io::Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(PathBuf::from(path));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            return Ok(dir.join("control.sock"));
        }
    }
    default_socket_path().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "cannot determine home directory for control socket",
        )
    })
}

/// Spawn the IPC server on a Unix domain socket.
///
/// The socket file is created with `0o600` permissions so only the owning
/// user can connect.  If the socket file already exists it is removed first.
#[cfg(test)]
fn prepare_ipc_server(socket_path: &Path) -> Arc<IpcServerHandle> {
    prepare_ipc_server_with_budget(
        socket_path,
        zero_runtime::configuration::ControlBudget::new(
            zero_runtime::configuration::ControlBudgetLimits::from_runtime(
                &zero_config::RuntimeOptionsConfig::default(),
            ),
        ),
    )
}
pub fn prepare_ipc_server_with_budget(
    socket_path: &Path,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
) -> Arc<IpcServerHandle> {
    Arc::new(IpcServerHandle {
        shutdown: std::sync::Mutex::new(None),
        task: crate::ipc::owner::ControlTask::empty(),
        listener: ControlledUnixListenerOwner::new(
            socket_path.to_owned(),
            CancellationToken::new(),
        ),
        originals: Arc::new(UnixOriginals::default()),
        path: socket_path.to_owned(),
        started: std::sync::atomic::AtomicBool::new(false),
        retired: std::sync::atomic::AtomicBool::new(false),
        budget,
    })
}
impl IpcServerHandle {
    pub(crate) async fn start(&self, handle: ProxyHandle) -> io::Result<()> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(io::Error::other("IPC original owner already started"));
        }
        let registration = self.task.reserve()?;
        let socket_path = &self.path;
        if socket_path.exists() {
            std::fs::remove_file(socket_path)?;
        }
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let listener = match self.listener.bind() {
            Ok(listener) => listener,
            Err(error) => {
                self.listener.close(super::owner::close_deadline()).await?;
                return Err(error);
            }
        };
        let permissions = (|| {
            let metadata = socket_path.metadata()?;
            let mut perms = metadata.permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(socket_path, perms)
        })();
        if let Err(error) = permissions {
            drop(listener);
            let closed = self.listener.close(super::owner::close_deadline()).await?;
            self.listener.remove_bound_path(&closed)?;
            return Err(error);
        }
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(shutdown_tx);
        let originals = self.originals.clone();
        let budget = self.budget.clone();
        let (begin, started) = oneshot::channel();
        let task = tokio::spawn(async move {
            started
                .await
                .map_err(|_| io::Error::other("IPC startup gate closed"))?;
            run_ipc_server(listener, handle, shutdown_rx, originals, budget).await
        });
        registration.install(task);
        begin
            .send(())
            .map_err(|_| io::Error::other("original IPC task stopped during startup"))?;
        info!(socket = %socket_path.display(), "ipc server ready");
        Ok(())
    }
}
#[cfg(test)]
async fn spawn_ipc_server(
    handle: ProxyHandle,
    socket_path: &Path,
) -> io::Result<Arc<IpcServerHandle>> {
    let owner = prepare_ipc_server(socket_path);
    owner.start(handle).await?;
    Ok(owner)
}

async fn run_ipc_server(
    mut listener: ControlledUnixListener,
    handle: ProxyHandle,
    mut shutdown: oneshot::Receiver<()>,
    originals: Arc<UnixOriginals>,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    let stop = CancellationToken::new();
    let mut failure = None;
    let active = Arc::new(AtomicU64::new(0));

    let work = super::owner::catch_work(async {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_result = listener.accept_with_admission(|| originals.before(&budget), || originals.no_native()) => {
                let (mut pending, control) = match accept_result {
                    Ok(value) => value,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                        continue;
                    }
                    Err(error) => { failure.get_or_insert(error); break; }
                };
                // Preserve both the real accepted original and its pre-native
                // charge before activation can suspend or fail.
                let charge = originals.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take().expect("IPC original charged before native accept");
                *originals.activating.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(UnixOriginal { control: tokio::sync::Mutex::new(control), charge: std::sync::Mutex::new(Some(charge)) }));
                let stream = match pending.activate(super::owner::close_deadline()).await {
                    Ok(stream) => stream,
                    Err(error) => { failure.get_or_insert(error); break; }
                };
                drop(pending);
                let peer_addr = "unix";
                let handle = handle.clone();
                let active = active.clone();
                let pipe = format!("{:?}", peer_addr);
                active.fetch_add(1, Ordering::Relaxed);
                super::events::emit_connected(&handle, active.load(Ordering::Relaxed), &pipe);
                debug!(?peer_addr, active = active.load(Ordering::Relaxed),
                      "ipc client connected");

                let connection_stop = stop.child_token();
                let connection_budget = budget.clone();
                let task = connections.spawn(async move {
                    let emit_handle = handle.clone();
                    let result = connection::handle_ipc_connection_with_budget_until(stream, handle, connection_stop, connection_budget).await;

                    let n = active.fetch_sub(1, Ordering::Relaxed) - 1;
                    let error_str = match &result {
                        Ok(_) => None,
                        Err(ref e) if connection::is_transient_disconnect(e) => Some(e.to_string()),
                        Err(ref e) => Some(e.to_string()),
                    };
                    super::events::emit_disconnected(&emit_handle, n, &pipe, error_str.as_deref());
                    match result {
                        Ok(()) => {
                            debug!(?peer_addr, active = n, "ipc client disconnected cleanly");
                        }
                        Err(ref error) if connection::is_transient_disconnect(error) => {
                            warn!(?peer_addr, error = %error, active = n,
                                  "ipc client disconnected");
                        }
                        Err(error) => {
                            warn!(?peer_addr, error = %error, active = n,
                                  "ipc connection failed");
                        }
                    }
                });
                let original = originals.activating.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take().expect("accepted original installed before activation");
                originals.active.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(task.id(), original);
            }
            result = connections.join_next_with_id(), if !connections.is_empty() => {
                if let Some(result) = result {
                    settle_connection(result, &originals, &mut failure).await;
                }
            }
        }
    }

    }).await;
    stop.cancel();
    drop(listener);
    while let Some(result) = connections.join_next_with_id().await {
        settle_connection(result, &originals, &mut failure).await;
    }
    if let Err(error) = originals.close_all().await {
        failure.get_or_insert(error);
    }
    if let Err(payload) = work {
        std::panic::resume_unwind(payload);
    }
    info!("ipc server stopped");
    failure.map_or(Ok(()), Err)
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
}

#[derive(Default)]
struct UnixOriginals {
    admission: std::sync::Mutex<Option<zero_runtime::configuration::ControlBudgetPermit>>,
    activating: std::sync::Mutex<Option<Arc<UnixOriginal>>>,
    active: std::sync::Mutex<std::collections::HashMap<tokio::task::Id, Arc<UnixOriginal>>>,
}
struct UnixOriginal {
    control: tokio::sync::Mutex<ControlledTcpControl>,
    charge: std::sync::Mutex<Option<zero_runtime::configuration::ControlBudgetPermit>>,
}
impl UnixOriginals {
    fn before(&self, budget: &Arc<zero_runtime::configuration::ControlBudget>) -> io::Result<()> {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if admission.is_some() {
            return Err(io::Error::other("IPC retains an unknown native accept"));
        }
        *admission = Some(budget.reserve(zero_runtime::configuration::ControlCharge::Connection)?);
        Ok(())
    }
    fn no_native(&self) {
        self.admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
    async fn close(original: &UnixOriginal) -> io::Result<()> {
        let mut control = original.control.lock().await;
        control.cancel(CloseReason::Shutdown);
        let report = control
            .wait_closed(super::owner::close_deadline())
            .await
            .map_err(io::Error::other)?;
        if !report.is_complete() {
            return Err(io::Error::other("IPC accepted original close incomplete"));
        }
        original
            .charge
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        Ok(())
    }
    async fn close_all(&self) -> io::Result<()> {
        let mut failure = None;
        let activating = self
            .activating
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(original) = activating {
            match Self::close(&original).await {
                Ok(()) => {
                    self.activating
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take();
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        let originals: Vec<_> = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(id, original)| (*id, original.clone()))
            .collect();
        for (id, original) in originals {
            match Self::close(&original).await {
                Ok(()) => {
                    self.active
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&id);
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
async fn settle_connection(
    result: Result<(tokio::task::Id, ()), tokio::task::JoinError>,
    originals: &UnixOriginals,
    failure: &mut Option<io::Error>,
) {
    let id = match result {
        Ok((id, ())) => id,
        Err(error) => {
            let id = error.id();
            failure.get_or_insert_with(|| io::Error::other(error));
            id
        }
    };
    let original = originals
        .active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&id)
        .cloned();
    if let Some(original) = original {
        match UnixOriginals::close(&original).await {
            Ok(()) => {
                originals
                    .active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&id);
            }
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/unix_originals.rs"]
mod original_tests;
