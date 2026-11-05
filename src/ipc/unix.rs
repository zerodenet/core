use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
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
}

impl IpcServerHandle {
    pub(crate) async fn wait_terminated(&self) -> io::Result<()> {
        self.task.join().await
    }
    pub async fn shutdown(&self) -> io::Result<()> {
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
        let closed = self.listener.close(super::owner::close_deadline()).await?;
        if !self.listener.matches_closed(&closed) {
            return Err(io::Error::other("IPC listener closed a foreign original"));
        }
        self.listener.remove_bound_path(&closed)?;
        accepted?;
        result
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
pub async fn spawn_ipc_server(
    handle: ProxyHandle,
    socket_path: &Path,
) -> io::Result<IpcServerHandle> {
    if socket_path.exists() {
        std::fs::remove_file(socket_path)?;
    }

    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let owner = ControlledUnixListenerOwner::new(socket_path.to_owned(), CancellationToken::new());
    let listener = match owner.bind() {
        Ok(listener) => listener,
        Err(error) => {
            owner.close(super::owner::close_deadline()).await?;
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
        let closed = owner.close(super::owner::close_deadline()).await?;
        owner.remove_bound_path(&closed)?;
        return Err(error);
    }

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let originals = Arc::new(UnixOriginals::default());
    let task_originals = originals.clone();
    let task =
        tokio::spawn(
            async move { run_ipc_server(listener, handle, shutdown_rx, task_originals).await },
        );

    info!(socket = %socket_path.display(), "ipc server ready");

    Ok(IpcServerHandle {
        shutdown: std::sync::Mutex::new(Some(shutdown_tx)),
        task: crate::ipc::owner::ControlTask::new(task),
        listener: owner,
        originals,
    })
}

async fn run_ipc_server(
    mut listener: ControlledUnixListener,
    handle: ProxyHandle,
    mut shutdown: oneshot::Receiver<()>,
    originals: Arc<UnixOriginals>,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    let stop = CancellationToken::new();
    let mut failure = None;
    let active = Arc::new(AtomicU64::new(0));

    let work = super::owner::catch_work(async {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_result = listener.accept() => {
                let (mut pending, control) = match accept_result { Ok(value) => value, Err(error) => { failure.get_or_insert(error); break; } };
                // Preserve the real accepted original before the first await.
                *originals.activating.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(tokio::sync::Mutex::new(control)));
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
                let task = connections.spawn(async move {
                    let emit_handle = handle.clone();
                    let result = connection::handle_ipc_connection_until(stream, handle, connection_stop).await;

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
    activating: std::sync::Mutex<Option<Arc<tokio::sync::Mutex<ControlledTcpControl>>>>,
    active: std::sync::Mutex<
        std::collections::HashMap<tokio::task::Id, Arc<tokio::sync::Mutex<ControlledTcpControl>>>,
    >,
}
impl UnixOriginals {
    async fn close(original: &tokio::sync::Mutex<ControlledTcpControl>) -> io::Result<()> {
        let mut original = original.lock().await;
        original.cancel(CloseReason::Shutdown);
        let report = original
            .wait_closed(super::owner::close_deadline())
            .await
            .map_err(io::Error::other)?;
        if !report.is_complete() {
            return Err(io::Error::other("IPC accepted original close incomplete"));
        }
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
mod original_tests {
    use super::*;
    use std::{
        future::{poll_fn, Future},
        task::Poll,
        time::Duration,
    };
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn cancelled_ipc_shutdown_waiter_retains_actual_subscriber_and_native_originals() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sock");
        let proxy = zero_proxy::Proxy::new(
            zero_config::RuntimeConfig::parse(
                r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        let engine = zero_engine::EngineHandle::new(proxy.engine().clone());
        let server = spawn_ipc_server(ProxyHandle::new(engine, proxy), &path)
            .await
            .unwrap();
        let client = tokio::net::UnixStream::connect(&path).await.unwrap();
        let mut client = BufReader::new(client);
        client
            .get_mut()
            .write_all(b"{\"type\":\"subscribe\",\"events\":[\"test.never\"]}\n")
            .await
            .unwrap();
        let mut ack = String::new();
        client.read_line(&mut ack).await.unwrap();
        assert!(
            serde_json::from_str::<serde_json::Value>(&ack).unwrap()["ok"]
                .as_bool()
                .unwrap()
        );
        let mut stopping = Box::pin(server.shutdown());
        poll_fn(|cx| {
            assert!(stopping.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(stopping);
        tokio::time::timeout(Duration::from_secs(2), server.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(!path.exists());
        assert!(server.originals.active.lock().unwrap().is_empty());
        assert!(server.originals.activating.lock().unwrap().is_none());
        let closed = server
            .listener
            .close(super::super::owner::close_deadline())
            .await
            .unwrap();
        assert!(closed.had_native_acquisition());
        assert!(server.listener.matches_closed(&closed));
        assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    }
}
