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
mod event_source;
mod messages;
mod native;
mod security;
mod source;
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
    let requests = Arc::new(messages::Requests::new(budget.clone()));
    let subscriptions = Arc::new(Subscriptions::new(budget));
    Ok(Arc::new(GrpcHandle {
        address: addr,
        local_addr: Mutex::new(addr),
        auth: security.auth,
        tls,
        started: AtomicBool::new(false),
        retired: AtomicBool::new(false),
        shutdown: Mutex::new(None),
        task: ControlTask::empty(),
        originals: Arc::new(GrpcOriginals {
            original: tokio::sync::Mutex::new(original),
            scope: tokio::sync::Mutex::new(scope),
            subscriptions,
            stop: tokio_util::sync::CancellationToken::new(),
            connections: Arc::new(native::connections::Connections::default()),
            requests,
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
    requests: Arc<messages::Requests>,
}
/// Retains the actual server, Scope, native controls and subscription jobs.
pub struct GrpcHandle {
    address: SocketAddr,
    local_addr: Mutex<SocketAddr>,
    auth: Option<GrpcServerAuth>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    started: AtomicBool,
    retired: AtomicBool,
    shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    task: ControlTask,
    originals: Arc<GrpcOriginals>,
}
impl GrpcHandle {
    /// Completion only after exact server/worker joins and native/accepted close.
    pub fn retirement_complete(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }
    pub fn local_addr(&self) -> SocketAddr {
        *self.local_addr.lock().unwrap()
    }
    pub async fn start<H>(&self, handle: H) -> Result<(), Box<dyn std::error::Error>>
    where
        H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
    {
        self.start_original(handle, source::OriginalSource::default())
            .await
    }
    /// Borrow production configuration/policy values from this same Engine.
    pub async fn start_with_engine<H>(
        &self,
        handle: H,
        engine: zero_engine::EngineHandle,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
    {
        self.start_original(
            handle,
            source::OriginalSource {
                engine: Some(engine),
                ..Default::default()
            },
        )
        .await
    }
    /// Use this exact production Proxy's guarded state and its same Engine.
    #[cfg(feature = "proxy-control")]
    pub async fn start_with_proxy<H>(
        &self,
        handle: H,
        proxy: zero_proxy::ProxyHandle,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
    {
        let engine = proxy.engine_handle().clone();
        self.start_original(
            handle,
            source::OriginalSource {
                engine: Some(engine),
                proxy: Some(proxy),
            },
        )
        .await
    }
    async fn start_original<H>(
        &self,
        handle: H,
        source: source::OriginalSource,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
    {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(std::io::Error::other("gRPC original owner already started").into());
        }
        let registration = self.task.reserve()?;
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
        let (begin, started) = tokio::sync::oneshot::channel();
        registration.install(tokio::spawn(async move {
            started
                .await
                .map_err(|_| std::io::Error::other("gRPC startup gate closed"))?;
            native::run(listener, handle, source, auth, tls, shutdown_rx, originals).await
        }));
        begin
            .send(())
            .map_err(|_| std::io::Error::other("original gRPC task stopped during startup"))?;
        info!(listen = %self.local_addr(), "gRPC api server ready");
        Ok(())
    }
    pub async fn wait_terminated(&self) -> std::io::Result<()> {
        self.task.join().await
    }
    pub async fn shutdown(&self) -> std::io::Result<()> {
        self.task.close_admission();
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
            .map_err(std::io::Error::other)
            .and_then(|report| tasks::confirmed_report("gRPC native", report));
        let scope = tasks::confirmed_report("gRPC Scope", tasks);
        let requests = self.originals.requests.confirm_released();
        if scope.is_ok() && native.is_ok() && requests.is_ok() {
            self.originals.connections.confirm_all_closed();
            self.retired.store(true, Ordering::Release);
        }
        tasks::finish([server, jobs, scope, native, requests])
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

mod subscriptions;
use subscriptions::Subscriptions;
mod service;
use service::ControlService;
