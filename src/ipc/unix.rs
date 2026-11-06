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
mod originals;
mod server;
use server::run_ipc_server;

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
        super::owner::finish_work([result, accepted, native, self.originals.terminal_faults()])
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
    terminal_faults: std::sync::Mutex<Vec<Arc<io::Error>>>,
}
struct UnixOriginal {
    control: tokio::sync::Mutex<ControlledTcpControl>,
    charge: std::sync::Mutex<Option<zero_runtime::configuration::ControlBudgetPermit>>,
}
#[cfg(test)]
#[path = "tests/unix_originals.rs"]
mod original_tests;
