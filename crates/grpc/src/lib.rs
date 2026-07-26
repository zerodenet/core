//! gRPC control plane server for Zero.
//!
//! Wraps the existing `ProxyHandle` (CommandService + QueryService + EventSource)
//! behind a gRPC endpoint.  All payloads are JSON-encoded, preserving full
//! compatibility with the HTTP and IPC API surfaces.

use std::net::SocketAddr;

use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use zero_api::event::EventFilter;
use zero_api::{CommandService, EventSource, EventStream, QueryService};

mod pb {
    tonic::include_proto!("zero.api.v1");
}
mod security;

pub use security::{GrpcServerAuth, GrpcServerSecurity, GrpcServerTls};

use pb::{
    control_server::{Control, ControlServer},
    Event as PbEvent, ExecuteRequest, ExecuteResponse, QueryRequest, QueryResponse,
    SubscribeRequest,
};

#[cfg(test)]
mod tests;

// ── Public API ─────────────────────────────────────────────────────────────

/// Start the gRPC server on `addr` with optional native TLS, mTLS, and bearer
/// authentication.
pub async fn spawn<H>(
    handle: H,
    addr: SocketAddr,
    security: GrpcServerSecurity,
) -> Result<GrpcHandle, Box<dyn std::error::Error>>
where
    H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
{
    security.validate_for(addr)?;
    let svc = ControlServer::new(ControlService::new(handle, security.auth.clone()));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let mut server = tonic::transport::Server::builder();
    if let Some(tls) = &security.tls {
        // The workspace can enable more than one rustls backend through
        // independent dependencies. Select Zero's ring backend explicitly
        // when no process-wide provider has been installed yet.
        let _ = rustls::crypto::ring::default_provider().install_default();
        server = server.tls_config(tls.tonic_config())?;
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        if let Err(e) = server
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        {
            error!(%e, "gRPC server exited with error");
        }
    });

    info!(listen = %local_addr, "gRPC api server ready");

    Ok(GrpcHandle {
        local_addr,
        shutdown: shutdown_tx,
        task,
    })
}

/// Handle to a running gRPC server. Shuts down on drop.
pub struct GrpcHandle {
    local_addr: SocketAddr,
    shutdown: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl GrpcHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.task.await;
    }
}

// ── Service ────────────────────────────────────────────────────────────────

struct ControlService<H> {
    handle: H,
    auth: Option<GrpcServerAuth>,
}

impl<H> ControlService<H> {
    fn new(handle: H, auth: Option<GrpcServerAuth>) -> Self {
        Self { handle, auth }
    }

    fn is_authorized<T>(&self, request: &Request<T>) -> bool {
        match &self.auth {
            Some(auth) => auth.is_authorized(request),
            None => true,
        }
    }
}

#[tonic::async_trait]
impl<H> Control for ControlService<H>
where
    H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
{
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        if !self.is_authorized(&request) {
            return Err(Status::unauthenticated(
                "missing or invalid bearer authorization",
            ));
        }
        let req = request.into_inner();
        let query: zero_api::QueryRequest = serde_json::from_slice(&req.payload)
            .map_err(|e| Status::invalid_argument(format!("query parse error: {e}")))?;
        let result = self
            .handle
            .query(query)
            .map_err(|e: zero_api::ApiError| Status::internal(e.to_string()))?;
        let json = serde_json::to_vec(&result).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(QueryResponse { payload: json }))
    }

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        if !self.is_authorized(&request) {
            return Err(Status::unauthenticated(
                "missing or invalid bearer authorization",
            ));
        }
        let req = request.into_inner();
        let cmd: zero_api::CommandRequest = serde_json::from_slice(&req.payload)
            .map_err(|e| Status::invalid_argument(format!("command parse error: {e}")))?;
        let result = self
            .handle
            .execute_acknowledged(cmd)
            .await
            .map_err(|e: zero_api::ApiError| Status::internal(e.to_string()))?;
        let json = serde_json::to_vec(&result).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ExecuteResponse { payload: json }))
    }

    type SubscribeStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<PbEvent, Status>> + Send + 'static>,
    >;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        if !self.is_authorized(&request) {
            return Err(Status::unauthenticated(
                "missing or invalid bearer authorization",
            ));
        }
        let req = request.into_inner();
        let filter = EventFilter {
            event_types: req.event_types,
            principal_keys: Vec::new(),
            inbound_tags: Vec::new(),
        };

        let subscriber = self
            .handle
            .subscribe(filter)
            .map_err(|e: zero_api::ApiError| Status::internal(format!("subscribe failed: {e}")))?;

        let (tx, rx) = mpsc::channel::<Result<PbEvent, Status>>(64);

        std::thread::spawn(move || {
            #[allow(clippy::while_let_loop)]
            loop {
                match subscriber.recv() {
                    Some(event) => {
                        let pb_event = PbEvent {
                            event_type: event.event_type,
                            event_id: event.event_id,
                            occurred_at: event.occurred_at_unix_ms,
                            payload: serde_json::to_vec(&event.payload).unwrap_or_default(),
                        };
                        if tx.blocking_send(Ok(pb_event)).is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
    }
}
