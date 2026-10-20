//! Startup preparation and confirmed failure cleanup.
mod dns;
use super::{logging::log_started, state::OrchestrationState};
use crate::groups::UrlTestRuntime;
use crate::runtime::route_runtime::{InboundListenerRuntimeFactory, SharedIngressRuntimeServices};
use crate::runtime::{listeners, reload, Proxy};
use std::collections::HashMap;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info};
use zero_engine::EngineError;

impl OrchestrationState {
    #[cfg(test)]
    pub(super) async fn new(proxy: &Proxy) -> Result<Self, EngineError> {
        let mut state = Self::assemble(proxy);
        if let Err(error) = state.start(proxy).await {
            return Err(state.finish(proxy, Err(error)).await.unwrap_err());
        }
        Ok(state)
    }

    pub(super) fn assemble(proxy: &Proxy) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Subscribe before capturing: a later direct Engine reload is queued
        // for reconciliation instead of being lost during startup awaits.
        let reload_async_rx = reload::subscribe_reload_bridge(proxy.engine.subscribe_reload());
        let applied_snapshot = proxy.engine.runtime_snapshot();
        let source_dir = applied_snapshot
            .config()
            .source_dir()
            .map(|path| path.to_path_buf());
        let tcp_services = proxy.tcp_runtime_services_for_snapshot(applied_snapshot.clone());
        let urltest_runtime = UrlTestRuntime::new(tcp_services.clone());
        let preparing_inbounds =
            std::sync::Arc::new(crate::runtime::inbound_owners::PreparingInbounds::default());
        let inbound_runtime_factory = InboundListenerRuntimeFactory::new(
            SharedIngressRuntimeServices::new(tcp_services),
            proxy.inbound_owners.clone(),
        )
        .with_preparing(preparing_inbounds.clone());
        let state = Self {
            preparing_inbounds,
            #[cfg(feature = "raw-ip-runtime")]
            egress_updates: proxy.egress_interface.subscribe_generation(),
            #[cfg(feature = "raw-ip-runtime")]
            device_generation: proxy.egress_interface.generation(),
            #[cfg(feature = "raw-ip-runtime")]
            device_retry_at: None,
            #[cfg(feature = "raw-ip-runtime")]
            device_retry_delay: std::time::Duration::from_secs(1),
            #[cfg(feature = "raw-ip-runtime")]
            outbound_devices: proxy.protocols.clone(),
            engine: proxy.engine.clone(),
            finished: false,
            orchestration_ready: proxy.orchestration_ready.clone(),
            shutdown_tx,
            shutdown_rx,
            listeners: JoinSet::new(),
            expected_listener_exits: 0,
            listener_stops: HashMap::new(),
            active_inbounds: applied_snapshot
                .config()
                .inbounds
                .iter()
                .filter(|inbound| {
                    zero_engine::EndpointAdmission::from_snapshot(&applied_snapshot)
                        .listener_enabled(&inbound.tag)
                })
                .map(|inbound| (inbound.tag.clone(), inbound.clone()))
                .collect(),
            urltests: JoinSet::new(),
            services: Default::default(),
            reload_async_rx,
            source_dir,
            urltest_runtime,
            inbound_runtime_factory,
            applied_snapshot,
            pending_dns_reload: None,
            configured_tun_failures: proxy.configured_tun_failures.subscribe(),
        };

        state.publish_starting(proxy, &state.applied_snapshot);
        state
    }

    pub(super) async fn start(&mut self, proxy: &Proxy) -> Result<(), EngineError> {
        self.preparing_inbounds.ensure_open()?;
        let snapshot = self.applied_snapshot.clone();
        let config = snapshot.config().clone();
        proxy.protocols.validate_dial_environment(&config)?;
        dns::prepare(proxy, &config).await?;
        self.preparing_inbounds.ensure_open()?;
        // Reuse the existing orchestration grace period. The migrated Direct
        // component starts no I/O or tasks; this is not a new admission budget.
        proxy
            .protocols
            .start_components(
                config.clone(),
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .await?;
        self.preparing_inbounds.ensure_open()?;
        // A cold diagnostic may have temporarily owned and then closed the
        // initially prepared plan while startup awaited the catalog gate.
        // Capture the actual retained started generation after that gate.
        let tcp_services = proxy.tcp_runtime_services_for_snapshot(snapshot.clone());
        self.urltest_runtime = UrlTestRuntime::new(tcp_services.clone());
        self.inbound_runtime_factory = InboundListenerRuntimeFactory::new(
            SharedIngressRuntimeServices::new(tcp_services),
            proxy.inbound_owners.clone(),
        )
        .with_preparing(self.preparing_inbounds.clone());
        #[cfg(feature = "managed-stream-runtime")]
        let prepared_services = proxy.protocols.prepare_inbound_services(&config)?;
        proxy
            .reconcile_configured_tun(config.runtime.tun.as_ref(), config.runtime.network.mtu)
            .await?;
        #[cfg(feature = "raw-ip-runtime")]
        let prepared_generation = proxy.egress_interface.generation();
        #[cfg(feature = "raw-ip-runtime")]
        let prepared_outbound_devices = proxy
            .protocols
            .prepare_outbound_devices(
                &config,
                proxy.tcp_runtime_services_for_snapshot(snapshot.clone()),
            )
            .await?;
        #[cfg(feature = "raw-ip-runtime")]
        for prepared in prepared_outbound_devices {
            prepared.publish().await;
        }
        #[cfg(feature = "raw-ip-runtime")]
        {
            self.device_generation = prepared_generation;
            // Leave startup notifications queued. A topology change racing
            // preparation completion must stay visible to the runtime loop.
        }
        self.start_inbounds(proxy).await?;
        #[cfg(feature = "managed-stream-runtime")]
        self.services
            .replace(
                prepared_services,
                proxy.tcp_runtime_services_for_snapshot(snapshot.clone()),
                self.shutdown_rx.clone(),
            )
            .await;
        self.preparing_inbounds.ensure_open()?;
        proxy
            .protocols
            .commit_components(&config, || Ok(()))
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
        self.inbound_runtime_factory = self.inbound_runtime_factory.clone().without_preparing();
        self.preparing_inbounds = Default::default();
        self.start_urltests();
        self.publish_endpoint_facts(proxy);
        log_started(proxy, &config);
        proxy.mark_orchestration_ready();

        Ok(())
    }

    async fn start_inbounds(&mut self, proxy: &Proxy) -> Result<(), EngineError> {
        let source_dir = self.source_dir.clone();
        let snapshot = self.applied_snapshot.clone();
        for inbound in &snapshot.config().inbounds {
            if !zero_engine::EndpointAdmission::from_snapshot(&snapshot)
                .listener_enabled(&inbound.tag)
            {
                continue;
            }
            let (tx, _) = watch::channel(false);
            self.listener_stops.insert(inbound.tag.clone(), tx.clone());
            let bound = listeners::bind_inbound_listener(
                &proxy.protocols,
                &self.inbound_runtime_factory,
                source_dir.as_deref(),
                inbound,
                tx.clone(),
            )
            .await?;
            listeners::spawn_inbound_listener(
                &proxy.protocols,
                source_dir.as_deref(),
                &self.inbound_runtime_factory,
                inbound,
                bound,
                tx,
                &mut self.listeners,
            )?
            .wait_ready()
            .await?;
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
