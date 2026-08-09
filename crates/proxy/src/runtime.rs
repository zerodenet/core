use std::future::Future;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::oneshot;
use tracing::{info, warn};
use zero_config::RuntimeConfig;
use zero_dns::DnsSystem;
use zero_engine::{Engine, EngineError};

use crate::inventory::ProtocolInventory;
use crate::protocol_registry::TcpRuntimeServices;

#[cfg(feature = "managed-datagram-runtime")]
pub(crate) mod datagram_udp;
mod handle;
pub(crate) mod http_redirect;
pub(crate) mod inbound_fallback;
pub(crate) use inbound_fallback::{
    prepare_inbound_route_accept, InboundFallbackTarget, PreparedInboundFallback,
    PreparedInboundRouteAccept,
};
pub(crate) mod inbound_operation;
pub(crate) mod inbound_route;
pub(crate) mod listener_loop;
mod listeners;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod mux_session;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod mux_tcp;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod mux_udp;
pub(crate) mod orchestration;
pub(crate) mod outbound_probe;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod packet_session_udp;
mod passive_relay_health;
pub(crate) mod path;
pub(crate) mod pipe;
pub(crate) mod principal_rate_limit;
mod reload;
pub(crate) mod route_runtime;
mod running;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod stream_udp;
pub(crate) mod tcp_dispatch;
pub(crate) mod tcp_ingress;
#[cfg(any(
    feature = "tcp-tunnel-runtime",
    feature = "tcp-session-runtime",
    feature = "managed-stream-runtime"
))]
pub(crate) mod transport_leaf;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) mod udp_association;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_delivery;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_dispatch;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_flow;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_ingress;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_socket;

pub use handle::{ConfigApplyReconciler, ConfigReconcileResult, ProxyHandle};
pub use running::RunningProxy;

#[derive(Debug, Clone)]
pub struct Proxy {
    engine: Engine,
    pub(crate) config: Arc<RuntimeConfig>,
    pub(crate) resolver: Arc<DnsSystem>,
    pub(crate) protocols: ProtocolInventory,
    pub(crate) tun_shutdown: Arc<std::sync::Mutex<Option<tokio::sync::watch::Sender<bool>>>>,
    pub(crate) tun_info: Arc<std::sync::Mutex<Option<TunInfo>>>,
    orchestration_ready: tokio::sync::watch::Sender<bool>,
    reload_ack: Arc<std::sync::Mutex<Option<PendingReloadAck>>>,
    reload_apply_lock: Arc<tokio::sync::Mutex<()>>,
    principal_rate_limits: principal_rate_limit::PrincipalRateLimitRegistry,
}

#[derive(Debug)]
struct PendingReloadAck {
    expected: RuntimeConfig,
    persist: bool,
    sender: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Clone)]
pub(crate) struct TunInfo {
    pub name: String,
    pub addr: String,
    pub mtu: u16,
    pub tag: String,
}

impl Proxy {
    pub fn new(config: RuntimeConfig) -> Result<Self, EngineError> {
        Self::from_engine(Engine::new(config)?)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, EngineError> {
        let config = RuntimeConfig::load_from_path(path)?;
        Self::new(config)
    }

    pub fn from_engine(engine: Engine) -> Result<Self, EngineError> {
        let protocols = ProtocolInventory::default();
        let config = engine.config();
        protocols.validate_config(&config)?;
        let dns = DnsSystem::build(config.runtime.dns.as_ref()).map_err(EngineError::Io)?;
        let (orchestration_ready, _) = tokio::sync::watch::channel(false);
        Ok(Self {
            config,
            engine,
            resolver: Arc::new(dns),
            protocols,
            tun_shutdown: Arc::new(std::sync::Mutex::new(None)),
            tun_info: Arc::new(std::sync::Mutex::new(None)),
            orchestration_ready,
            reload_ack: Arc::new(std::sync::Mutex::new(None)),
            reload_apply_lock: Arc::new(tokio::sync::Mutex::new(())),
            principal_rate_limits: principal_rate_limit::PrincipalRateLimitRegistry::default(),
        })
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub(crate) fn mark_orchestration_ready(&self) {
        self.orchestration_ready.send_replace(true);
    }

    pub(crate) fn complete_reload(&self, expected: &RuntimeConfig, result: Result<(), String>) {
        let pending = {
            let mut pending = self.reload_ack.lock().expect("reload ack lock poisoned");
            if pending
                .as_ref()
                .is_some_and(|pending| pending.expected == *expected)
            {
                pending.take()
            } else {
                None
            }
        };
        if let Some(pending) = pending {
            let _ = pending.sender.send(result);
        }
    }

    pub(crate) fn pending_reload_persists(&self, expected: &RuntimeConfig) -> bool {
        self.reload_ack
            .lock()
            .expect("reload ack lock poisoned")
            .as_ref()
            .filter(|pending| pending.expected == *expected)
            .is_none_or(|pending| pending.persist)
    }

    pub(crate) fn tcp_runtime_services(&self) -> TcpRuntimeServices {
        self.tcp_runtime_services_for_snapshot(self.engine.runtime_snapshot())
    }

    pub(crate) fn tcp_runtime_services_for_snapshot(
        &self,
        snapshot: Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> TcpRuntimeServices {
        TcpRuntimeServices::new(
            self.engine().clone(),
            snapshot,
            self.resolver.clone(),
            self.protocols.clone(),
            self.principal_rate_limits.clone(),
        )
    }

    #[cfg(all(test, feature = "udp-runtime"))]
    pub(crate) fn udp_runtime_services(&self) -> crate::protocol_registry::UdpRuntimeServices {
        crate::protocol_registry::UdpRuntimeServices::new(self.tcp_runtime_services())
    }

    pub fn with_udp_upstream_idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.engine = self.engine.with_udp_upstream_idle_timeout(timeout);
        self
    }

    pub fn into_engine(self) -> Engine {
        self.engine
    }

    pub fn spawn(&self) -> RunningProxy {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let proxy = self.clone();
        let task = tokio::spawn(async move {
            proxy
                .run_until(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        RunningProxy {
            proxy: self.clone(),
            shutdown: Some(shutdown_tx),
            task,
        }
    }

    pub async fn run(&self) -> Result<(), EngineError> {
        self.run_until(async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => info!("shutdown signal received"),
                Err(error) => warn!(error = %error, "failed to listen for ctrl-c; stopping proxy"),
            }
        })
        .await
    }

    pub async fn run_until<F>(&self, shutdown: F) -> Result<(), EngineError>
    where
        F: Future<Output = ()> + Send,
    {
        orchestration::run_until(self, shutdown).await
    }

    pub async fn probe_outbound_single(
        &self,
        target_tag: &str,
        url: &str,
    ) -> Result<u64, EngineError> {
        let request = outbound_probe::OutboundProbeRequest::parse(url)
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
        outbound_probe::OutboundProbeRuntime::new(self.tcp_runtime_services())
            .probe_target_tag(target_tag, &request)
            .await
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))
    }
}

impl Deref for Proxy {
    type Target = Engine;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}
