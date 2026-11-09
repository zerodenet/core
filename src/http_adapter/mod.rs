mod auth;
mod body;
mod connection;
mod connections;
mod lifecycle;
mod listener;
mod server;
pub use lifecycle::prepare_http_server_with_budget;
pub mod handlers;
mod input;
mod output;
pub mod ratelimit;
pub mod response;
pub mod router;
pub mod sse;

use std::io;
use std::time::Duration;

use std::{num::NonZeroUsize, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tracing::{debug, info, warn};
use zero_api::{AuthContext, Permission};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::CancellationToken;
use zero_platform_tokio::contract::{
    component::{AcceptedTcpLimits, ResourceDeclaration},
    scope::{TaskClass, TaskScope},
    ChannelError, Impact, Reason, Stage,
};
use zero_platform_tokio::{AcceptedTcpClaim, ControlledTcpListener, ControlledTcpListenerOwner};
use zero_proxy::ProxyHandle;

use router::{HttpRequest, RouteResult};

pub struct HttpServerHandle {
    shutdown: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    task: crate::ipc::owner::ControlTask,
    original: Arc<tokio::sync::Mutex<Option<ControlledTcpListenerOwner>>>,
    scope: Arc<tokio::sync::Mutex<TaskScope>>,
    listen: String,
    auth: Option<Arc<HttpServerAuth>>,
    started: std::sync::atomic::AtomicBool,
    retired: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    bound: std::sync::Mutex<Option<std::net::SocketAddr>>,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
    connections: Arc<connections::Connections>,
    report_failures: Arc<crate::ipc::owner::ReportReceiver>,
}

/// A single token entry parsed from configuration.
#[derive(Debug, Clone)]
struct TokenEntry {
    name: String,
    key: String,
    permissions: Vec<Permission>,
}

/// Multi-token authentication store for the HTTP API.
///
/// If `tokens` is empty (no auth configured), all requests are treated as
/// admin for local-only control plane usage.
#[derive(Debug, Clone)]
pub struct HttpServerAuth {
    tokens: Vec<TokenEntry>,
}

impl HttpServerAuth {
    /// Create an auth store with a single admin-scoped key.
    pub fn single_admin(api_key: String) -> Self {
        Self {
            tokens: vec![TokenEntry {
                name: "default".to_owned(),
                key: api_key,
                permissions: vec![
                    Permission::Read,
                    Permission::Control,
                    Permission::Config,
                    Permission::Admin,
                ],
            }],
        }
    }

    fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

fn component_io(error: io::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(error)
}

fn is_transient_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
}

#[cfg(test)]
#[path = "tests/retirement.rs"]
mod retirement_tests;

#[cfg(test)]
#[path = "tests/envelopes.rs"]
mod envelope_tests;

#[cfg(test)]
#[path = "tests/endpoints.rs"]
mod endpoint_tests;
