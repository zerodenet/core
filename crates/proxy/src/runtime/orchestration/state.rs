use std::collections::HashMap;
use std::path::PathBuf;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use zero_config::{InboundConfig, RuntimeConfig};
use zero_engine::EngineError;

use super::logging::{log_reload_reconciled, log_started};
use crate::groups::UrlTestRuntime;
use crate::runtime::route_runtime::{InboundListenerRuntimeFactory, SharedIngressRuntimeServices};
use crate::runtime::{listeners, reload, Proxy};

pub(super) struct OrchestrationState {
    pub(super) shutdown_tx: watch::Sender<bool>,
    pub(super) shutdown_rx: watch::Receiver<bool>,
    pub(super) listeners: JoinSet<Result<(), EngineError>>,
    pub(super) expected_listener_exits: usize,
    pub(super) listener_stops: HashMap<String, watch::Sender<bool>>,
    pub(super) active_inbounds: HashMap<String, InboundConfig>,
    pub(super) urltests: JoinSet<Result<(), EngineError>>,
    pub(super) reload_async_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    pub(super) source_dir: Option<PathBuf>,
    pub(super) urltest_runtime: UrlTestRuntime,
    pub(super) inbound_runtime_factory: InboundListenerRuntimeFactory,
    pub(super) applied_config: std::sync::Arc<RuntimeConfig>,
}

impl OrchestrationState {
    pub(super) async fn new(proxy: &Proxy) -> Result<Self, EngineError> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let source_dir = proxy.config.source_dir().map(|path| path.to_path_buf());
        let tcp_services = proxy.tcp_runtime_services();
        let urltest_runtime = UrlTestRuntime::new(tcp_services.clone());
        let inbound_runtime_factory =
            InboundListenerRuntimeFactory::new(SharedIngressRuntimeServices::new(tcp_services));
        let mut state = Self {
            shutdown_tx,
            shutdown_rx,
            listeners: JoinSet::new(),
            expected_listener_exits: 0,
            listener_stops: HashMap::new(),
            active_inbounds: proxy
                .config
                .inbounds
                .iter()
                .map(|inbound| (inbound.tag.clone(), inbound.clone()))
                .collect(),
            urltests: JoinSet::new(),
            reload_async_rx: reload::subscribe_reload_bridge(proxy.engine.subscribe_reload()),
            source_dir,
            urltest_runtime,
            inbound_runtime_factory,
            applied_config: proxy.config.clone(),
        };

        state.start_inbounds(proxy).await?;
        state.start_urltests();
        log_started(proxy);
        proxy.mark_orchestration_ready();

        Ok(state)
    }

    pub(super) fn is_idle(&self) -> bool {
        self.listeners.is_empty() && self.urltests.is_empty()
    }

    pub(super) fn propagate_shutdown(&self) {
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

    pub(super) async fn reconcile_reload(&mut self, proxy: &Proxy) {
        let new_snapshot = proxy.engine.runtime_snapshot();
        let new_config = new_snapshot.config().clone();
        let candidate_tcp_services = proxy.tcp_runtime_services_for_snapshot(new_snapshot);
        let candidate_runtime_factory = InboundListenerRuntimeFactory::new(
            SharedIngressRuntimeServices::new(candidate_tcp_services.clone()),
        );
        let candidate_urltest_runtime = UrlTestRuntime::new(candidate_tcp_services);
        let rollback_runtime_factory = self.inbound_runtime_factory.clone();
        let source_dir = self.source_dir.clone();
        if let Err(error) = proxy.resolver.reload(new_config.runtime.dns.as_ref()) {
            warn!(%error, reason = "dns_reload_error", "failed to reload dns config");
        }
        let inbound_result = listeners::reconcile_inbounds(
            &proxy.protocols,
            source_dir.as_deref(),
            &candidate_runtime_factory,
            &rollback_runtime_factory,
            &new_config,
            listeners::InboundReconcileState {
                listener_stops: &mut self.listener_stops,
                active_inbounds: &mut self.active_inbounds,
                expected_listener_exits: &mut self.expected_listener_exits,
                listeners: &mut self.listeners,
            },
        )
        .await;
        if let Err(error) = inbound_result {
            let message = error.to_string();
            warn!(
                core_instance_id = proxy.core_instance_id(),
                config_revision = proxy.config_revision(),
                reason = "listener_reconcile_error",
                %error,
                "config reload listener reconciliation failed; restoring last known-good config"
            );
            let persist = proxy.pending_reload_persists(&new_config);
            let rollback = if persist {
                proxy.engine.stage_config((*self.applied_config).clone())
            } else {
                proxy
                    .engine
                    .stage_runtime_config((*self.applied_config).clone())
            };
            let acknowledgement = if let Err(rollback_error) = rollback {
                warn!(
                    core_instance_id = proxy.core_instance_id(),
                    config_revision = proxy.config_revision(),
                    reason = "reload_rollback_error",
                    %rollback_error,
                    "failed to restore last known-good config after reload failure"
                );
                format!("{message}; last-known-good config restore failed: {rollback_error}")
            } else {
                message
            };
            proxy.complete_reload(&new_config, Err(acknowledgement));
            return;
        }
        listeners::reconcile_urltests(
            &candidate_urltest_runtime,
            &self.shutdown_rx,
            &mut self.urltests,
        )
        .await;
        proxy.protocols.on_config_reloaded(&new_config);
        self.inbound_runtime_factory = candidate_runtime_factory;
        self.urltest_runtime = candidate_urltest_runtime;
        self.applied_config = new_config.clone();
        log_reload_reconciled(&new_config);
        proxy.complete_reload(&new_config, Ok(()));
    }

    async fn start_inbounds(&mut self, proxy: &Proxy) -> Result<(), EngineError> {
        let source_dir = self.source_dir.clone();
        for inbound in &proxy.config.inbounds {
            let (tx, rx) = watch::channel(false);
            self.listener_stops.insert(inbound.tag.clone(), tx);
            let bound =
                listeners::bind_inbound_listener(&proxy.protocols, source_dir.as_deref(), inbound)
                    .await?;
            listeners::spawn_inbound_listener(
                &proxy.protocols,
                source_dir.as_deref(),
                &self.inbound_runtime_factory,
                inbound,
                bound,
                rx,
                &mut self.listeners,
            )?;
        }
        Ok(())
    }

    fn start_urltests(&mut self) {
        for group_id in self.urltest_runtime.group_ids() {
            let runtime = self.urltest_runtime.clone();
            let shutdown = self.shutdown_rx.clone();
            self.urltests.spawn(async move {
                info!(group_id = group_id.index(), "urltest runtime task started");
                let result = runtime.run_urltest_group(group_id, shutdown).await;
                match &result {
                    Ok(()) => info!(
                        group_id = group_id.index(),
                        reason = "urltest_task_returned",
                        "urltest runtime task returned"
                    ),
                    Err(urltest_error) => error!(
                        group_id = group_id.index(),
                        reason = "urltest_task_error",
                        error = %urltest_error,
                        "urltest runtime task failed"
                    ),
                }
                result
            });
        }
    }
}
