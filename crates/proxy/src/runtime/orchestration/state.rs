use std::collections::HashMap;
use std::path::PathBuf;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::info;
use zero_config::InboundConfig;
use zero_engine::EngineError;

use crate::groups::UrlTestRuntime;
use crate::runtime::route_runtime::InboundListenerRuntimeFactory;

pub(super) struct OrchestrationState {
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) egress_updates: watch::Receiver<u64>,
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) device_generation: u64,
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) device_retry_at: Option<tokio::time::Instant>,
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) device_retry_delay: std::time::Duration,
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) outbound_devices: crate::inventory::ProtocolInventory,
    pub(super) engine: zero_engine::Engine,
    pub(super) finished: bool,
    pub(super) orchestration_ready: watch::Sender<bool>,
    pub(super) shutdown_tx: watch::Sender<bool>,
    pub(super) shutdown_rx: watch::Receiver<bool>,
    pub(super) listeners: JoinSet<Result<(), EngineError>>,
    pub(super) expected_listener_exits: usize,
    pub(super) listener_stops: HashMap<String, watch::Sender<bool>>,
    pub(super) active_inbounds: HashMap<String, InboundConfig>,
    pub(super) urltests: JoinSet<Result<(), EngineError>>,
    pub(super) services: crate::runtime::inbound_service::InboundServices,
    pub(super) reload_async_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    pub(super) source_dir: Option<PathBuf>,
    pub(super) urltest_runtime: UrlTestRuntime,
    pub(super) inbound_runtime_factory: InboundListenerRuntimeFactory,
    pub(super) applied_snapshot: std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
    pub(super) configured_tun_failures: tokio::sync::broadcast::Receiver<String>,
}

impl OrchestrationState {
    pub(super) fn propagate_shutdown(&self) {
        self.orchestration_ready.send_replace(false);
        let _ = self.shutdown_tx.send(true);
        for tx in self.listener_stops.values() {
            let _ = tx.send(true);
        }
        info!(
            listener_tasks = self.listeners.len(),
            urltest_tasks = self.urltests.len(),
            reason = "proxy_shutdown",
            "propagated proxy shutdown to background tasks"
        );
    }
}
