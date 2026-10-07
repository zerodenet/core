//! Startup preparation and confirmed failure cleanup.
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
    pub(super) async fn new(proxy: &Proxy) -> Result<Self, EngineError> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let source_dir = proxy.config.source_dir().map(|path| path.to_path_buf());
        let tcp_services = proxy.tcp_runtime_services();
        let urltest_runtime = UrlTestRuntime::new(tcp_services.clone());
        let inbound_runtime_factory =
            InboundListenerRuntimeFactory::new(SharedIngressRuntimeServices::new(tcp_services));
        let mut state = Self {
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
            active_inbounds: proxy
                .config
                .inbounds
                .iter()
                .filter(|inbound| {
                    zero_engine::EndpointAdmission::from_snapshot(&proxy.engine.runtime_snapshot())
                        .listener_enabled(&inbound.tag)
                })
                .map(|inbound| (inbound.tag.clone(), inbound.clone()))
                .collect(),
            urltests: JoinSet::new(),
            services: Default::default(),
            reload_async_rx: reload::subscribe_reload_bridge(proxy.engine.subscribe_reload()),
            source_dir,
            urltest_runtime,
            inbound_runtime_factory,
            applied_snapshot: proxy.engine.runtime_snapshot(),
            configured_tun_failures: proxy.configured_tun_failures.subscribe(),
        };

        state.publish_starting(proxy, &state.applied_snapshot);
        if let Err(error) = state.start(proxy).await {
            return Err(state.finish(proxy, Err(error)).await.unwrap_err());
        }
        Ok(state)
    }

    async fn start(&mut self, proxy: &Proxy) -> Result<(), EngineError> {
        #[cfg(feature = "managed-stream-runtime")]
        let prepared_services = proxy.protocols.prepare_inbound_services(&proxy.config)?;
        proxy
            .reconcile_configured_tun(
                proxy.config.runtime.tun.as_ref(),
                proxy.config.runtime.network.mtu,
            )
            .await?;
        #[cfg(feature = "raw-ip-runtime")]
        let prepared_generation = proxy.egress_interface.generation();
        #[cfg(feature = "raw-ip-runtime")]
        let prepared_outbound_devices = proxy
            .protocols
            .prepare_outbound_devices(&proxy.config, proxy.tcp_runtime_services())
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
                proxy.tcp_runtime_services(),
                self.shutdown_rx.clone(),
            )
            .await;
        self.start_urltests();
        self.publish_endpoint_facts(proxy);
        log_started(proxy);
        proxy.mark_orchestration_ready();

        Ok(())
    }

    async fn start_inbounds(&mut self, proxy: &Proxy) -> Result<(), EngineError> {
        let source_dir = self.source_dir.clone();
        for inbound in &proxy.config.inbounds {
            if !zero_engine::EndpointAdmission::from_snapshot(&proxy.engine.runtime_snapshot())
                .listener_enabled(&inbound.tag)
            {
                continue;
            }
            let (tx, rx) = watch::channel(false);
            self.listener_stops.insert(inbound.tag.clone(), tx);
            let bound = listeners::bind_inbound_listener(
                &proxy.protocols,
                &self.inbound_runtime_factory,
                source_dir.as_deref(),
                inbound,
            )
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
