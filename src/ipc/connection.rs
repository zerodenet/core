//! Shared IPC connection handling logic.
//!
//! This module contains the transport-agnostic request/response loop,
//! command parsing, and utility functions used by both the Unix domain
//! socket and Windows named pipe IPC servers.

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use zero_api::{
    ApiResponse, AuthContext, CommandRequest, CommandService, EventFilter, EventSource, Permission,
    QueryService,
};
use zero_proxy::ProxyHandle;

use super::protocol::{ipc_api_error, ipc_error, ipc_ok, serialize_frame, IpcRequest};

type CommandParseError = Box<ApiResponse<()>>;
const MAX_PENDING_CONCURRENT_IPC_COMMANDS: usize = 32;

struct ConcurrentCommandTasks {
    tasks: JoinSet<()>,
    stop: CancellationToken,
    failure: Option<tokio::task::JoinError>,
}

impl ConcurrentCommandTasks {
    fn new(stop: CancellationToken) -> Self {
        Self {
            tasks: JoinSet::new(),
            stop,
            failure: None,
        }
    }

    fn reap_finished(&mut self) {
        while let Some(result) = self.tasks.try_join_next() {
            if let Err(error) = result {
                self.failure.get_or_insert(error);
            }
        }
    }

    fn try_spawn(&mut self, future: impl Future<Output = ()> + Send + 'static) -> bool {
        self.reap_finished();
        if self.tasks.len() >= MAX_PENDING_CONCURRENT_IPC_COMMANDS {
            return false;
        }
        let stop = self.stop.clone();
        self.tasks.spawn(async move {
            tokio::select! { _=stop.cancelled()=>{}, _=future=>{} }
        });
        true
    }

    async fn cancel_all(&mut self) -> io::Result<()> {
        self.stop.cancel();
        while let Some(result) = self.tasks.join_next().await {
            if let Err(error) = result {
                self.failure.get_or_insert(error);
            }
        }
        self.failure
            .take()
            .map_or(Ok(()), |error| Err(io::Error::other(error)))
    }
}

#[cfg(test)]
mod tests;

/// Parse a string method name + JSON params into a typed `CommandRequest`.
///
/// Uses the same serde `#[serde(tag = "method", content = "params")]` path as
/// the HTTP adapter — the two transport layers share one deserialization
/// definition.  Adding a new command variant only requires changing
/// `zero_api::CommandRequest`; no transport-specific code needed.
pub(crate) fn parse_command(
    method: &str,
    params: &serde_json::Value,
) -> Result<CommandRequest, CommandParseError> {
    let wrapper = serde_json::json!({
        "method": method,
        "params": params,
    });
    serde_json::from_value::<CommandRequest>(wrapper).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("unknown_variant") {
            Box::new(ApiResponse::error_msg(
                "unsupported",
                format!("unknown command method: {method}"),
            ))
        } else {
            Box::new(ApiResponse::error_msg("invalid_argument", msg))
        }
    })
}

/// Handle a single IPC connection: read JSON-line frames, dispatch
/// queries/commands/subscriptions, and write responses.
///
/// After a `Subscribe`, the connection stays open for event streaming
/// AND continues to accept `Query` / `Command` / `Ping` frames.
/// Responses echo the request `id` so the client can pair them on a
/// multiplexed connection.
pub(crate) async fn handle_ipc_connection<S>(stream: S, handle: ProxyHandle) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    handle_ipc_connection_until(stream, handle, CancellationToken::new()).await
}

pub(crate) async fn handle_ipc_connection_until<S>(
    stream: S,
    handle: ProxyHandle,
    stop: CancellationToken,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // IPC connections are protected by OS-level file permissions
    // (Unix socket 0o600).  All callers are trusted with admin.
    let auth_ctx = AuthContext {
        subject: Some("ipc-local".to_owned()),
        permissions: vec![
            Permission::Read,
            Permission::Control,
            Permission::Config,
            Permission::Admin,
        ],
    };

    handle_ipc_connection_with_auth_until(stream, handle, auth_ctx, stop).await
}

async fn handle_ipc_connection_with_auth<S>(
    stream: S,
    handle: ProxyHandle,
    auth_ctx: AuthContext,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    handle_ipc_connection_with_auth_until(stream, handle, auth_ctx, CancellationToken::new()).await
}

async fn handle_ipc_connection_with_auth_until<S>(
    stream: S,
    handle: ProxyHandle,
    auth_ctx: AuthContext,
    stop: CancellationToken,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(TokioMutex::new(BufWriter::new(writer)));

    let mut subscribed = false;
    let mut line = String::new();
    let mut bg_blocking: Option<tokio::task::JoinHandle<()>> = None;
    let mut bg_events: Option<tokio::task::JoinHandle<io::Result<()>>> = None;
    let mut concurrent_commands = ConcurrentCommandTasks::new(stop.clone());
    // Every error/cancellation exits this work future into the same retained
    // cleanup path. The original job handles live outside that future.
    let work = async {
        loop {
            line.clear();
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                break;
            }
            if line.trim().is_empty() {
                continue;
            }

            let request: IpcRequest = match serde_json::from_str(line.trim()) {
                Ok(req) => req,
                Err(_) => {
                    let resp = ipc_error(None, "invalid_argument", "invalid request frame");
                    write_ipc_response(&mut *writer.lock().await, &resp).await?;
                    continue;
                }
            };

            match request {
                IpcRequest::Ping { id } => {
                    let resp = ipc_ok(id, "pong");
                    write_ipc_response(&mut *writer.lock().await, &resp).await?;
                }
                IpcRequest::Query { id, request } => match handle.query(request) {
                    Ok(query_resp) => {
                        let value = serde_json::to_value(query_resp).map_err(io::Error::other)?;
                        let resp = ipc_ok(id, value);
                        write_ipc_response(&mut *writer.lock().await, &resp).await?;
                    }
                    Err(error) => {
                        let resp = ipc_api_error(id, &error);
                        write_ipc_response(&mut *writer.lock().await, &resp).await?;
                    }
                },
                IpcRequest::Command { id, method, params } => {
                    let command = parse_command(&method, &params);
                    match command {
                        Ok(ref cmd) if !auth_ctx.allows(cmd.required_permission()) => {
                            let error =
                                zero_api::ApiError::permission_denied(cmd.required_permission());
                            let resp = ipc_api_error(id, &error);
                            write_ipc_response(&mut *writer.lock().await, &resp).await?;
                        }
                        Ok(cmd) => {
                            if command_can_run_concurrently(&cmd) {
                                let command_handle = handle.clone();
                                let response_writer = writer.clone();
                                let rejected_id = id.clone();
                                let spawned = concurrent_commands.try_spawn(async move {
                                    let result = execute_ipc_command(command_handle, cmd).await;
                                    if let Err(error) =
                                        write_command_result(response_writer, id, result).await
                                    {
                                        if !is_transient_disconnect(&error) {
                                            tracing::warn!(
                                                %error,
                                                "failed to write concurrent IPC command response"
                                            );
                                        }
                                    }
                                });
                                if !spawned {
                                    let error = zero_api::ApiError::new(
                                    zero_api::ApiErrorCode::Conflict,
                                    format!(
                                        "this IPC connection already has {MAX_PENDING_CONCURRENT_IPC_COMMANDS} pending outbound probes"
                                    ),
                                );
                                    write_command_result(writer.clone(), rejected_id, Err(error))
                                        .await?;
                                }
                            } else {
                                let result = execute_ipc_command(handle.clone(), cmd).await;
                                write_command_result(writer.clone(), id, result).await?;
                            }
                        }
                        Err(error) => {
                            let resp = error.with_id(id);
                            write_ipc_response(&mut *writer.lock().await, &resp).await?;
                        }
                    }
                }
                IpcRequest::Subscribe { id, events } => {
                    if subscribed {
                        let resp = ipc_error(
                            id,
                            "already_subscribed",
                            "this connection is already subscribed to events",
                        );
                        write_ipc_response(&mut *writer.lock().await, &resp).await?;
                        continue;
                    }

                    let mut filter = EventFilter::default();
                    if let Some(types) = events {
                        filter.event_types = types;
                    }

                    // Ack only after the subscription actually succeeds, so the
                    // request `id` is moved exactly once (no clone) and
                    // "subscribed" truthfully reflects engine state.
                    match handle.subscribe(filter) {
                        Ok(subscriber) => {
                            subscribed = true;
                            let resp = ipc_ok(id, "subscribed");
                            write_ipc_response(&mut *writer.lock().await, &resp).await?;
                            // Tokio (async) channel so the writer task can
                            // `recv().await` without blocking a worker thread, and so
                            // aborting it cancels at the await point immediately.
                            let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(32);

                            let cancel_for_task = stop.clone();

                            // Drain ready events without throttling the subscription.
                            // Only idle polls sleep; the bounded bridge applies backpressure
                            // until the writer consumes events or drops its receiver.
                            bg_blocking = Some(tokio::task::spawn_blocking(move || {
                                loop {
                                    if cancel_for_task.is_cancelled() || event_tx.is_closed() {
                                        break;
                                    }
                                    if let Some(event) = subscriber.try_recv() {
                                        // Serialize the full ApiEvent envelope (same format
                                        // as SSE) so consumers use one parsing code path
                                        // for both channels.
                                        let value = serde_json::to_value(&event);
                                        if let Ok(value) = value {
                                            if event_tx.blocking_send(value).is_err() {
                                                break;
                                            }
                                        }
                                        continue;
                                    }
                                    std::thread::sleep(Duration::from_millis(25));
                                }
                            }));

                            // Writer task: pushes events and 30s heartbeats to the
                            // client. Runs concurrently with the main read loop so
                            // query/command/ping frames keep working after subscribe.
                            let event_writer = writer.clone();
                            let event_stop = stop.clone();
                            bg_events = Some(tokio::spawn(async move {
                                let work = async {
                                    loop {
                                        match tokio::time::timeout(
                                            Duration::from_secs(30),
                                            event_rx.recv(),
                                        )
                                        .await
                                        {
                                            Ok(Some(value)) => {
                                                let frame = serialize_frame(&value)
                                                    .map_err(io::Error::other)?;
                                                let mut w = event_writer.lock().await;
                                                w.write_all(&frame).await?;
                                                w.flush().await?;
                                            }
                                            Ok(None) => break, // sender dropped
                                            Err(_) => {
                                                // heartbeat (SSE comment line; clients ignore)
                                                let mut w = event_writer.lock().await;
                                                w.write_all(b":\n").await?;
                                                w.flush().await?;
                                            }
                                        }
                                    }
                                    io::Result::Ok(())
                                };
                                let result = tokio::select! {
                                    _ = event_stop.cancelled() => Ok(()),
                                    result = work => result,
                                };
                                event_stop.cancel();
                                result
                            }));
                        }
                        Err(error) => {
                            let resp = ipc_api_error(id, &error);
                            write_ipc_response(&mut *writer.lock().await, &resp).await?;
                        }
                    }
                }
            }
        }

        io::Result::Ok(())
    };
    let result = tokio::select! {
        _ = stop.cancelled() => Ok(()),
        result = work => result,
    };
    stop.cancel();
    // The event writer drops its actual receiver, waking a full blocking
    // bridge. Join both originals even when an earlier read/write failed.
    let events = match bg_events.take() {
        Some(task) => task
            .await
            .map_err(io::Error::other)
            .and_then(|result| result),
        None => Ok(()),
    };
    let bridge = match bg_blocking.take() {
        Some(task) => task.await.map_err(io::Error::other),
        None => Ok(()),
    };
    let commands = concurrent_commands.cancel_all().await;
    result?;
    events?;
    bridge?;
    commands
}

fn command_can_run_concurrently(command: &CommandRequest) -> bool {
    matches!(command, CommandRequest::DiagnosticsProbeOutbound(_))
}

async fn execute_ipc_command(
    command_handle: ProxyHandle,
    command: CommandRequest,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let command = match command {
        CommandRequest::DiagnosticsProbeOutbound(command) => {
            return command_handle
                .execute_diagnostics_probe_outbound_command(command)
                .await;
        }
        command => command,
    };
    // The service owns coordination and blocking execution. In particular,
    // endpoint controls and stats.reset must share config.apply's executor.
    command_handle.execute_acknowledged(command).await
}

async fn write_command_result<W>(
    writer: Arc<TokioMutex<BufWriter<W>>>,
    id: Option<serde_json::Value>,
    result: zero_api::ApiResult<zero_api::CommandResponse>,
) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    match result {
        Ok(command_response) => {
            let value = serde_json::to_value(command_response).map_err(io::Error::other)?;
            let response = ipc_ok(id, value);
            write_ipc_response(&mut *writer.lock().await, &response).await
        }
        Err(error) => {
            let response = ipc_api_error(id, &error);
            write_ipc_response(&mut *writer.lock().await, &response).await
        }
    }
}

/// Write a serialized IPC response frame to the transport.
///
/// The connection writer is wrapped in a `BufWriter`, so we MUST flush
/// after each frame — otherwise responses sit in the 8 KB buffer and the
/// client times out waiting. The only other flush site is the background
/// event/heartbeat task, which doesn't run until a `Subscribe` succeeds
/// and only fires every 30 s; relying on it leaves every pre-subscribe
/// response (and ack/error frames) stuck in the buffer indefinitely.
pub(crate) async fn write_ipc_response(
    writer: &mut (impl AsyncWriteExt + Unpin),
    response: &impl Serialize,
) -> io::Result<()> {
    let frame = serialize_frame(response).map_err(io::Error::other)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

/// Return true if the I/O error is from a routine client disconnect.
pub(crate) fn is_transient_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
}
