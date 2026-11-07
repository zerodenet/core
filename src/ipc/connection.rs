//! Shared IPC connection handling logic.
//!
//! This module contains the transport-agnostic request/response loop,
//! command parsing, and utility functions used by both the Unix domain
//! socket and Windows named pipe IPC servers.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Mutex as TokioMutex;
use zero_api::{
    ApiResponse, AuthContext, CommandRequest, CommandService, EventFilter, EventSource, Permission,
    QueryService,
};
use zero_platform_tokio::contract::CancellationToken;
use zero_proxy::ProxyHandle;
#[cfg(test)]
use zero_runtime::configuration::ControlBudgetLimits;
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};

#[cfg(test)]
use super::protocol::serialize_frame;
use super::protocol::{ipc_error, ipc_ok, IpcRequest};

mod command;
mod commands;
mod events;
mod frames;
mod input;
mod run;
#[cfg(test)]
use command::write_command_result;
#[cfg(test)]
pub(crate) use command::write_ipc_response;
use command::{command_can_run_concurrently, execute_ipc_command, write_budgeted_command_result};
use commands::ConcurrentCommandTasks;
pub(crate) use input::is_invalid_input_frame;
use run::handle_ipc_connection_with_auth_until;

type CommandParseError = Box<ApiResponse<()>>;
#[cfg(test)]
const MAX_PENDING_CONCURRENT_IPC_COMMANDS: usize = 32;

#[cfg(test)]
mod tests;

fn parse_owned_command(
    method: String,
    params: serde_json::Value,
) -> Result<CommandRequest, CommandParseError> {
    let mut wrapper = serde_json::Map::new();
    wrapper.insert("method".into(), serde_json::Value::String(method));
    wrapper.insert("params".into(), params);
    serde_json::from_value(serde_json::Value::Object(wrapper)).map_err(|error| {
        Box::new(ApiResponse::error_msg(
            "invalid_argument",
            error.to_string(),
        ))
    })
}

/// Handle a single IPC connection: read JSON-line frames, dispatch
/// queries/commands/subscriptions, and write responses.
///
/// After a `Subscribe`, the connection stays open for event streaming
/// AND continues to accept `Query` / `Command` / `Ping` frames.
/// Responses echo the request `id` so the client can pair them on a
/// multiplexed connection.
#[cfg(test)]
pub(crate) async fn handle_ipc_connection<S>(stream: S, handle: ProxyHandle) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    handle_ipc_connection_until(stream, handle, CancellationToken::new()).await
}

#[cfg(test)]
async fn handle_ipc_connection_until<S>(
    stream: S,
    handle: ProxyHandle,
    stop: CancellationToken,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let budget = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    handle_ipc_connection_with_budget_until(stream, handle, stop, budget).await
}
#[cfg(test)]
pub(crate) async fn handle_ipc_connection_with_budget_until<S>(
    stream: S,
    handle: ProxyHandle,
    stop: CancellationToken,
    budget: Arc<ControlBudget>,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let original = budget.reserve(ControlCharge::Connection)?;
    let local = original
        .connection_budget()
        .expect("same original connection issuer");
    handle_ipc_connection_with_connection_until(stream, handle, stop, budget, local).await
}

pub(crate) async fn handle_ipc_connection_with_connection_until<S>(
    stream: S,
    handle: ProxyHandle,
    stop: CancellationToken,
    budget: Arc<ControlBudget>,
    local: zero_runtime::configuration::ControlConnectionBudget,
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

    handle_ipc_connection_with_auth_until(stream, handle, auth_ctx, stop, budget, local).await
}

#[cfg(all(test, unix, feature = "wireguard"))]
async fn handle_ipc_connection_with_auth<S>(
    stream: S,
    handle: ProxyHandle,
    auth_ctx: AuthContext,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let budget = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    let original = budget.reserve(ControlCharge::Connection)?;
    let local = original.connection_budget().unwrap();
    handle_ipc_connection_with_auth_until(
        stream,
        handle,
        auth_ctx,
        CancellationToken::new(),
        budget,
        local,
    )
    .await
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
