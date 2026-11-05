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
mod budget;
mod native;
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

/// Pure owner preparation. The application installs this original before
/// `start` may bind or register any listener/accepted protocol task.
pub fn prepare(
    addr: SocketAddr,
    security: GrpcServerSecurity,
) -> Result<Arc<GrpcHandle>, Box<dyn std::error::Error>> {
    prepare_with_budget(addr, security, budget::defaults())
}

pub fn prepare_with_budget(
    addr: SocketAddr,
    security: GrpcServerSecurity,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
) -> Result<Arc<GrpcHandle>, Box<dyn std::error::Error>> {
    security.validate_for(addr)?;
    let tls = security
        .tls
        .as_ref()
        .map(GrpcServerTls::native_acceptor)
        .transpose()?;
    let (scope, original) = native::cold(addr)?;
    let subscriptions = Arc::new(Subscriptions::new(budget));
    Ok(Arc::new(GrpcHandle {
        address: addr,
        local_addr: Mutex::new(addr),
        auth: security.auth,
        tls,
        started: AtomicBool::new(false),
        shutdown: Mutex::new(None),
        task: ControlTask::empty(),
        originals: Arc::new(GrpcOriginals {
            original: tokio::sync::Mutex::new(original),
            scope: tokio::sync::Mutex::new(scope),
            subscriptions,
            stop: tokio_util::sync::CancellationToken::new(),
            connections: Arc::new(native::connections::Connections::default()),
        }),
    }))
}

pub async fn spawn<H>(
    handle: H,
    addr: SocketAddr,
    security: GrpcServerSecurity,
) -> Result<Arc<GrpcHandle>, Box<dyn std::error::Error>>
where
    H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
{
    let owner = prepare(addr, security)?;
    owner.start(handle).await?;
    Ok(owner)
}
struct GrpcOriginals {
    original: tokio::sync::Mutex<zero_platform_tokio::ControlledTcpListenerOwner>,
    scope: tokio::sync::Mutex<zero_platform_tokio::contract::scope::TaskScope>,
    subscriptions: Arc<Subscriptions>,
    stop: tokio_util::sync::CancellationToken,
    connections: Arc<native::connections::Connections>,
}
/// Retains the actual server, Scope, native controls and subscription jobs.
pub struct GrpcHandle {
    address: SocketAddr,
    local_addr: Mutex<SocketAddr>,
    auth: Option<GrpcServerAuth>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    started: AtomicBool,
    shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    task: ControlTask,
    originals: Arc<GrpcOriginals>,
}
impl GrpcHandle {
    pub fn local_addr(&self) -> SocketAddr {
        *self.local_addr.lock().unwrap()
    }
    pub async fn start<H>(&self, handle: H) -> Result<(), Box<dyn std::error::Error>>
    where
        H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
    {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(std::io::Error::other("gRPC original owner already started").into());
        }
        let listener = self
            .originals
            .original
            .lock()
            .await
            .bind_listener(self.address)?;
        *self.local_addr.lock().unwrap() = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        *self.shutdown.lock().unwrap() = Some(shutdown_tx);
        let originals = self.originals.clone();
        let auth = self.auth.clone();
        let tls = self.tls.clone();
        self.task.install(tokio::spawn(async move {
            native::run(listener, handle, auth, tls, shutdown_rx, originals).await
        }))?;
        info!(listen = %self.local_addr(), "gRPC api server ready");
        Ok(())
    }
    pub async fn wait_terminated(&self) -> std::io::Result<()> {
        self.task.join().await
    }
    pub async fn shutdown(&self) -> std::io::Result<()> {
        self.originals
            .subscriptions
            .stop
            .store(true, Ordering::Release);
        if let Some(stop) = self.shutdown.lock().unwrap().take() {
            let _ = stop.send(());
        }
        let server = self.task.join().await;
        let jobs = self.originals.subscriptions.join_all().await;
        let tasks = self
            .originals
            .scope
            .lock()
            .await
            .shutdown(native::deadline())
            .await;
        let native = self
            .originals
            .original
            .lock()
            .await
            .wait_closed(native::deadline())
            .await
            .map_err(std::io::Error::other)?;
        if !tasks.is_complete() || !native.is_complete() {
            return Err(std::io::Error::other(
                "gRPC actual Scope or native original remains retained",
            ));
        }
        self.originals.connections.confirm_all_closed();
        server?;
        jobs
    }
}
impl Drop for GrpcHandle {
    fn drop(&mut self) {
        self.originals
            .subscriptions
            .stop
            .store(true, Ordering::Release);
        self.originals.stop.cancel();
        if let Some(stop) = self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = stop.send(());
        }
    }
}

// ── Service ────────────────────────────────────────────────────────────────

struct Subscriptions {
    stop: AtomicBool,
    jobs: Mutex<Vec<Arc<ControlTask>>>,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
}
impl Default for Subscriptions {
    fn default() -> Self {
        Self::new(budget::defaults())
    }
}
impl Subscriptions {
    fn new(budget: Arc<zero_runtime::configuration::ControlBudget>) -> Self {
        Self {
            stop: AtomicBool::new(false),
            jobs: Mutex::new(Vec::new()),
            budget,
        }
    }
    async fn join_all(&self) -> std::io::Result<()> {
        let tasks = self.jobs.lock().unwrap().clone();
        let mut failure = None;
        for job in tasks {
            if let Err(error) = job.join().await {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
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
        let subscription_charge = self
            .subscriptions
            .budget
            .reserve(zero_runtime::configuration::ControlCharge::Subscription)
            .map_err(|error| Status::resource_exhausted(error.to_string()))?;
        let subscriber = self
            .handle
            .subscribe(filter)
            .map_err(|e: zero_api::ApiError| Status::internal(format!("subscribe failed: {e}")))?;

        let (tx, rx) = mpsc::channel::<budget::QueuedEvent>(64);

        let subscriptions = self.subscriptions.clone();
        let mut jobs = subscriptions.jobs.lock().unwrap();
        if subscriptions.stop.load(Ordering::Acquire) {
            return Err(Status::unavailable("control server is stopping"));
        }
        let owner = subscriptions.clone();
        // The retained job waits with a finite idle wake and keeps a full
        // event until the bounded channel accepts it. Stop and disconnected
        // consumers never depend on another source event to unblock recv.
        jobs.push(Arc::new(ControlTask::with_charge(
            tokio::task::spawn_blocking(move || {
                while !owner.stop.load(Ordering::Acquire) && !tx.is_closed() {
                    let event = match subscriber.recv_timeout(Duration::from_millis(25)) {
                        zero_api::EventStreamReceive::Event(event) => event,
                        zero_api::EventStreamReceive::Timeout => continue,
                        zero_api::EventStreamReceive::Closed => break,
                    };
                    let mut message = PbEvent {
                        event_type: event.event_type,
                        event_id: event.event_id,
                        occurred_at: event.occurred_at_unix_ms,
                        payload: Vec::new(),
                    };
                    let bytes = budget::frame_len(&message, &event.payload)?;
                    let Some(charge) = budget::reserve(&owner, bytes, || tx.is_closed())? else {
                        return Ok(());
                    };
                    message.payload =
                        serde_json::to_vec(&event.payload).map_err(std::io::Error::other)?;
                    let mut pending = budget::QueuedEvent {
                        event: Ok(message),
                        _charge: Some(charge),
                    };
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
            }),
            subscription_charge,
        )));
        drop(jobs);

        let stream = budget::Events(rx);
        Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
    }
}
