//! Windows Named Pipe IPC server and client.
//!
//! Named Pipes live in the `\\.\pipe\` namespace — they are not filesystem
//! objects, so the path is always the well-known `\\.\pipe\zero-control`
//! unless overridden via CLI.

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::windows::named_pipe::ServerOptions;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use zero_proxy::ProxyHandle;

use super::connection;
use super::protocol::{serialize_frame, IpcRequest, IpcResponse};

pub const PIPE_NAME: &str = r"\\.\pipe\zero-control";

pub struct IpcServerHandle {
    shutdown: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    task: crate::ipc::owner::ControlTask,
    path: PathBuf,
    started: std::sync::atomic::AtomicBool,
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
        self.task.join().await
    }
}

pub fn default_socket_path() -> Option<PathBuf> {
    Some(PathBuf::from(PIPE_NAME))
}

pub fn resolve_socket_path(explicit: Option<&str>) -> io::Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(PathBuf::from(path));
    }
    Ok(PathBuf::from(PIPE_NAME))
}

pub fn prepare_ipc_server(pipe_path: &std::path::Path) -> Arc<IpcServerHandle> {
    Arc::new(IpcServerHandle {
        shutdown: std::sync::Mutex::new(None),
        task: crate::ipc::owner::ControlTask::empty(),
        path: pipe_path.to_owned(),
        started: std::sync::atomic::AtomicBool::new(false),
    })
}
impl IpcServerHandle {
    pub(crate) async fn start(&self, handle: ProxyHandle) -> io::Result<()> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(io::Error::other("IPC original owner already started"));
        }
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(shutdown_tx);
        let pipe = self.path.to_string_lossy().to_string();
        self.task.install(tokio::spawn(async move {
            run_ipc_server(&pipe, handle, shutdown_rx).await
        }))?;
        info!(pipe = %self.path.display(), "ipc server ready");
        Ok(())
    }
}
pub async fn spawn_ipc_server(
    handle: ProxyHandle,
    pipe_path: &std::path::Path,
) -> io::Result<Arc<IpcServerHandle>> {
    let owner = prepare_ipc_server(pipe_path);
    owner.start(handle).await?;
    Ok(owner)
}

async fn run_ipc_server(
    pipe_name: &str,
    handle: ProxyHandle,
    mut shutdown: oneshot::Receiver<()>,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    let stop = CancellationToken::new();
    let mut failure = None;
    let active = Arc::new(AtomicU64::new(0));

    loop {
        let server = match ServerOptions::new().create(pipe_name) {
            Ok(s) => s,
            Err(e) => {
                error!(pipe = %pipe_name, error = %e, "failed to create named pipe");
                tokio::select! { _ = &mut shutdown => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                continue;
            }
        };

        tokio::select! {
            _ = &mut shutdown => break,
            result = server.connect() => {
                if let Err(e) = result {
                    warn!(pipe = %pipe_name, error = %e, "named pipe connect failed");
                    continue;
                }

                let handle = handle.clone();
                let active = active.clone();
                let pipe_owned = pipe_name.to_string();
                active.fetch_add(1, Ordering::Relaxed);
                super::events::emit_connected(&handle, active.load(Ordering::Relaxed), &pipe_owned);
                debug!(pipe = %pipe_name, active = active.load(Ordering::Relaxed),
                      "ipc client connected");

                let connection_stop = stop.child_token();
                connections.spawn(async move {
                    let emit_handle = handle.clone();
                    let result = connection::handle_ipc_connection_until(server, handle, connection_stop).await;

                    let n = active.fetch_sub(1, Ordering::Relaxed) - 1;
                    let error_str = match &result {
                        Ok(_) => None,
                        Err(ref e) if connection::is_transient_disconnect(e) => Some(e.to_string()),
                        Err(ref e) => Some(e.to_string()),
                    };
                    super::events::emit_disconnected(&emit_handle, n, &pipe_owned, error_str.as_deref());
                    match result {
                        Ok(()) => {
                            debug!(active = n, "ipc client disconnected cleanly");
                        }
                        Err(ref error) if connection::is_transient_disconnect(error) => {
                            warn!(error = %error, active = n,
                                  "ipc client disconnected");
                        }
                        Err(error) => {
                            warn!(error = %error, active = n,
                                  "ipc connection failed");
                        }
                    }
                });
            }
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result {
                    if !error.is_cancelled() {
                        error!(error = %error, "ipc connection task panicked");
                        failure.get_or_insert_with(|| io::Error::other(error));
                    }
                }
            }
        }
    }

    stop.cancel();
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            if !error.is_cancelled() {
                error!(error = %error, "ipc connection task panicked during shutdown");
                failure.get_or_insert_with(|| io::Error::other(error));
            }
        }
    }

    info!("ipc server stopped");
    failure.map_or(Ok(()), Err)
}

// ── Client ────────────────────────────────────────────────────────────

use std::io::{BufRead as StdBufRead, BufReader as StdBufReader, Write};

/// Connect to the named pipe and send a single request.
pub fn send_request(pipe_name: &str, request: &IpcRequest) -> io::Result<IpcResponse> {
    let stream = open_pipe_client(pipe_name)?;
    send_frame(&stream, request)?;
    read_response(&stream)
}

/// Connect and stream events.
pub fn stream_events(
    pipe_name: &str,
    request: &IpcRequest,
    mut on_event: impl FnMut(serde_json::Value),
) -> io::Result<()> {
    let stream = open_pipe_client(pipe_name)?;
    send_frame(&stream, request)?;

    let reader = StdBufReader::new(&stream);
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(&line).map_err(io::Error::other)?;
        on_event(value);
    }
    Ok(())
}

fn open_pipe_client(pipe_name: &str) -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(pipe_name)
}

fn send_frame(mut stream: &std::fs::File, request: &IpcRequest) -> io::Result<()> {
    let frame = serialize_frame(request).map_err(io::Error::other)?;
    stream.write_all(&frame)?;
    Ok(())
}

fn read_response(stream: &std::fs::File) -> io::Result<IpcResponse> {
    let reader = StdBufReader::new(stream);
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        return serde_json::from_str(&line).map_err(io::Error::other);
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "no response from ipc pipe",
    ))
}
