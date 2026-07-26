//! Shared IPC connection handling logic.
//!
//! This module contains the transport-agnostic request/response loop,
//! command parsing, and utility functions used by both the Unix domain
//! socket and Windows named pipe IPC servers.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Mutex as TokioMutex;
use zero_api::{
    ApiResponse, AuthContext, CommandRequest, CommandService, EventFilter, EventSource, Permission,
    QueryService,
};
use zero_proxy::ProxyHandle;

use super::protocol::{ipc_api_error, ipc_error, ipc_ok, serialize_frame, IpcRequest};

type CommandParseError = Box<ApiResponse<()>>;

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
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(TokioMutex::new(BufWriter::new(writer)));

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

    let mut subscribed = false;
    let mut line = String::new();
    // Handles for the background tasks spawned by a `Subscribe` request.
    // Kept here so we can abort them when the connection ends, instead of
    // relying on a write failure up to 30s later to reap them.
    let mut bg_blocking: Option<tokio::task::JoinHandle<()>> = None;
    let mut bg_events: Option<tokio::task::JoinHandle<io::Result<()>>> = None;
    // Cancel flag shared with the blocking subscriber poller; set when the
    // connection ends so the poller exits within ~100ms instead of looping
    // forever (which would keep the engine pushing events at a dead client).
    let mut bg_cancel: Option<Arc<AtomicBool>> = None;
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
                        let command_handle = handle.clone();
                        let acknowledged_apply = matches!(
                            cmd,
                            CommandRequest::ConfigApply(_) | CommandRequest::ConfigApplyRuntime(_)
                        );
                        let result = if acknowledged_apply {
                            command_handle.execute_acknowledged(cmd).await
                        } else {
                            // Some synchronous commands bridge to blocking runtime
                            // services. Keep those off the reactor.
                            match tokio::task::spawn_blocking(move || command_handle.execute(cmd))
                                .await
                            {
                                Ok(result) => result,
                                Err(error) => Err(zero_api::ApiError::new(
                                    zero_api::ApiErrorCode::Internal,
                                    format!("IPC command task failed: {error}"),
                                )),
                            }
                        };
                        match result {
                            Ok(cmd_resp) => {
                                let value =
                                    serde_json::to_value(cmd_resp).map_err(io::Error::other)?;
                                let resp = ipc_ok(id, value);
                                write_ipc_response(&mut *writer.lock().await, &resp).await?;
                            }
                            Err(error) => {
                                let resp = ipc_api_error(id, &error);
                                write_ipc_response(&mut *writer.lock().await, &resp).await?;
                            }
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
                        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

                        // Cooperative cancel flag. `abort()` does not interrupt a
                        // `spawn_blocking` task mid-sleep, and `try_recv()` returning
                        // `None` cannot distinguish "empty" from "publisher gone" — so
                        // neither alone reaps the poller. The flag is set when the
                        // connection ends; the loop checks it each iteration and exits
                        // within ~100ms, dropping the subscriber so the engine stops
                        // pushing events at a dead connection.
                        let cancel = Arc::new(AtomicBool::new(false));
                        let cancel_for_task = cancel.clone();

                        // Poll with `try_recv()` + short sleep rather than a blocking
                        // `recv()`: the 100ms sleep is a responsive cancel checkpoint
                        // without holding a worker thread in a blocking call.
                        bg_blocking = Some(tokio::task::spawn_blocking(move || {
                            loop {
                                if cancel_for_task.load(Ordering::Relaxed) {
                                    break;
                                }
                                if let Some(event) = subscriber.try_recv() {
                                    // Serialize the full ApiEvent envelope (same format
                                    // as SSE) so consumers use one parsing code path
                                    // for both channels.
                                    let value = serde_json::to_value(&event);
                                    if let Ok(value) = value {
                                        if event_tx.send(value).is_err() {
                                            break;
                                        }
                                    }
                                }
                                std::thread::sleep(Duration::from_millis(100));
                            }
                        }));
                        bg_cancel = Some(cancel);

                        // Writer task: pushes events and 30s heartbeats to the
                        // client. Runs concurrently with the main read loop so
                        // query/command/ping frames keep working after subscribe.
                        let event_writer = writer.clone();
                        bg_events = Some(tokio::spawn(async move {
                            loop {
                                match tokio::time::timeout(Duration::from_secs(30), event_rx.recv())
                                    .await
                                {
                                    Ok(Some(value)) => {
                                        let frame =
                                            serialize_frame(&value).map_err(io::Error::other)?;
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

    // Connection ended (client EOF or shutdown). Tear down the background
    // tasks spawned by any Subscribe request so they don't linger holding a
    // subscriber (which would otherwise keep the engine pushing events at a
    // dead connection). The writer task cancels at its await point; setting
    // the cancel flag stops the blocking poller within ~100ms; abort is a
    // belt-and-suspenders fallback.
    if let Some(handle) = bg_events.take() {
        handle.abort();
    }
    if let Some(flag) = bg_cancel.take() {
        flag.store(true, Ordering::Relaxed);
    }
    if let Some(handle) = bg_blocking.take() {
        handle.abort();
    }

    Ok(())
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
