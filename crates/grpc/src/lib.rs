//! gRPC control plane server for Zero.
//!
//! Wraps the existing `ProxyHandle` (CommandService + QueryService + EventSource)
//! behind a gRPC endpoint.  All payloads are JSON-encoded, preserving full
//! compatibility with the HTTP and IPC API surfaces.

use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use tracing::info;

use zero_api::event::EventFilter;
use zero_api::{CommandService, EventSource, EventStream, QueryService};

mod pb {
    tonic::include_proto!("zero.api.v1");
}
mod security;
mod tasks;
use tasks::ControlTask;

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
    let subscriptions = Arc::new(Subscriptions::default());
    let svc = ControlServer::new(ControlService::new(
        handle,
        security.auth.clone(),
        subscriptions.clone(),
    ));

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
        server
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = shutdown_rx.await;
                },
            )
            .await
            .map_err(std::io::Error::other)
    });

    info!(listen = %local_addr, "gRPC api server ready");

    Ok(GrpcHandle {
        local_addr,
        shutdown: Mutex::new(Some(shutdown_tx)),
        task: ControlTask::new(task),
        subscriptions,
    })
}

/// Retains the actual server and subscription jobs until explicit shutdown.
pub struct GrpcHandle {
    local_addr: SocketAddr,
    shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    task: ControlTask,
    subscriptions: Arc<Subscriptions>,
}

impl GrpcHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn wait_terminated(&self) -> std::io::Result<()> {
        self.task.join().await
    }

    pub async fn shutdown(&self) -> std::io::Result<()> {
        self.subscriptions.stop.store(true, Ordering::Release);
        if let Some(stop) = self.shutdown.lock().unwrap().take() {
            let _ = stop.send(());
        }
        // Await both originals even if either failed. The tonic server owns
        // active command ACKs; subscription jobs own their actual subscribers.
        let server = self.task.join().await;
        let tasks = self.subscriptions.jobs.lock().unwrap().clone();
        let mut failure = None;
        for job in tasks {
            if let Err(error) = job.join().await {
                failure.get_or_insert(error);
            }
        }
        server?;
        failure.map_or(Ok(()), Err)
    }
}

// ── Service ────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Subscriptions {
    stop: AtomicBool,
    jobs: Mutex<Vec<Arc<ControlTask>>>,
}

impl Subscriptions {
    async fn reap_completed(&self) -> std::io::Result<()> {
        let originals = self.jobs.lock().unwrap().clone();
        let mut failure = None;
        for original in originals {
            if original.finished() {
                if let Err(error) = original.join().await {
                    failure.get_or_insert(error);
                }
            }
        }
        self.jobs
            .lock()
            .unwrap()
            .retain(|original| !original.joined_successfully());
        failure.map_or(Ok(()), Err)
    }
}

struct ControlService<H> {
    handle: H,
    auth: Option<GrpcServerAuth>,
    subscriptions: Arc<Subscriptions>,
}

impl<H> ControlService<H> {
    fn new(handle: H, auth: Option<GrpcServerAuth>, subscriptions: Arc<Subscriptions>) -> Self {
        Self {
            handle,
            auth,
            subscriptions,
        }
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

        self.subscriptions
            .reap_completed()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        let subscriber = self
            .handle
            .subscribe(filter)
            .map_err(|e: zero_api::ApiError| Status::internal(format!("subscribe failed: {e}")))?;

        let (tx, rx) = mpsc::channel::<Result<PbEvent, Status>>(64);

        let subscriptions = self.subscriptions.clone();
        let mut jobs = subscriptions.jobs.lock().unwrap();
        if subscriptions.stop.load(Ordering::Acquire) {
            return Err(Status::unavailable("control server is stopping"));
        }
        let owner = subscriptions.clone();
        // The retained job waits with a finite idle wake and keeps a full
        // event until the bounded channel accepts it. Stop and disconnected
        // consumers never depend on another source event to unblock recv.
        jobs.push(Arc::new(ControlTask::new(tokio::task::spawn_blocking(
            move || {
                while !owner.stop.load(Ordering::Acquire) && !tx.is_closed() {
                    let event = match subscriber.recv_timeout(Duration::from_millis(25)) {
                        zero_api::EventStreamReceive::Event(event) => event,
                        zero_api::EventStreamReceive::Timeout => continue,
                        zero_api::EventStreamReceive::Closed => break,
                    };
                    let mut pending = Ok(PbEvent {
                        event_type: event.event_type,
                        event_id: event.event_id,
                        occurred_at: event.occurred_at_unix_ms,
                        payload: serde_json::to_vec(&event.payload).unwrap_or_default(),
                    });
                    loop {
                        if owner.stop.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        match tx.try_send(pending) {
                            Ok(()) => break,
                            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                            Err(mpsc::error::TrySendError::Full(event)) => pending = event,
                        }
                        std::thread::sleep(Duration::from_millis(25));
                    }
                }
                Ok(())
            },
        ))));
        drop(jobs);

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
    }
}
