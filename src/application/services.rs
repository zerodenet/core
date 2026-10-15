use std::error::Error;
use std::sync::Arc;

use tokio::sync::Mutex;
use zero_config::RuntimeConfig;
use zero_engine::Engine;
use zero_proxy::{ConfigApplyReconciler, ConfigReconcileResult};

use crate::hooks;

mod dispatcher;
use dispatcher::{changed_components, dispatcher_enabled};
#[cfg(feature = "event-dispatcher")]
use dispatcher::{dispatcher_changed, spawn_status_monitor};

#[cfg(feature = "event-dispatcher")]
mod network;

#[cfg(feature = "event-dispatcher")]
use network::ApplicationEventSinkTcpDialer;

pub(super) struct ApplicationServices {
    engine: Engine,
    ipc_hook_socket: Option<String>,
    #[cfg(feature = "event-dispatcher")]
    event_network: zero_connector::EventDispatcherNetwork,
    state: Mutex<ApplicationServiceState>,
}

struct ApplicationServiceState {
    config: Arc<RuntimeConfig>,
    installed: bool,
    #[cfg(feature = "event-dispatcher")]
    dispatcher: Option<zero_connector::EventDispatcherHandle>,
    #[cfg(feature = "event-dispatcher")]
    continuation: Option<zero_connector::EventDispatcherContinuation>,
    #[cfg(feature = "event-dispatcher")]
    status_monitor: Option<tokio::task::JoinHandle<()>>,
    #[cfg(feature = "event-dispatcher")]
    engine_started_bootstrapped: bool,
}

impl ApplicationServices {
    #[cfg(all(test, feature = "sink-jsonl"))]
    pub(super) async fn start(
        engine: Engine,
        ipc_hook_socket: Option<&str>,
    ) -> Result<Arc<Self>, Box<dyn Error>> {
        #[cfg(feature = "event-dispatcher")]
        let event_network = zero_connector::EventDispatcherNetwork::system();
        Self::start_with_network(
            engine,
            ipc_hook_socket,
            #[cfg(feature = "event-dispatcher")]
            event_network,
        )
        .await
    }

    pub(super) async fn start_with_proxy(
        proxy: &zero_proxy::Proxy,
        ipc_hook_socket: Option<&str>,
    ) -> Result<Arc<Self>, Box<dyn Error>> {
        let engine = proxy.engine().clone();
        #[cfg(feature = "event-dispatcher")]
        let event_network = zero_connector::EventDispatcherNetwork::new(Arc::new(
            ApplicationEventSinkTcpDialer::new(proxy.egress_interface_control()),
        ));
        Self::start_with_network(
            engine,
            ipc_hook_socket,
            #[cfg(feature = "event-dispatcher")]
            event_network,
        )
        .await
    }

    async fn start_with_network(
        engine: Engine,
        ipc_hook_socket: Option<&str>,
        #[cfg(feature = "event-dispatcher")] event_network: zero_connector::EventDispatcherNetwork,
    ) -> Result<Arc<Self>, Box<dyn Error>> {
        let config = engine.config();
        let services = Arc::new(Self {
            engine,
            ipc_hook_socket: ipc_hook_socket.map(str::to_owned),
            #[cfg(feature = "event-dispatcher")]
            event_network,
            state: Mutex::new(ApplicationServiceState {
                config: config.clone(),
                installed: false,
                #[cfg(feature = "event-dispatcher")]
                dispatcher: None,
                #[cfg(feature = "event-dispatcher")]
                continuation: None,
                #[cfg(feature = "event-dispatcher")]
                status_monitor: None,
                #[cfg(feature = "event-dispatcher")]
                engine_started_bootstrapped: false,
            }),
        });

        services
            .validate(config.as_ref(), config.as_ref())
            .map_err(std::io::Error::other)?;
        {
            let mut state = services.state.lock().await;
            services
                .install_target(&mut state, config)
                .await
                .map_err(std::io::Error::other)?;
        }
        Ok(services)
    }

    pub(super) async fn shutdown_status_monitor(&self) {
        #[cfg(feature = "event-dispatcher")]
        let mut state = self.state.lock().await;
        #[cfg(feature = "event-dispatcher")]
        if let Some(monitor) = state.status_monitor.take() {
            monitor.abort();
        }
    }

    pub(super) async fn shutdown_dispatcher(&self) {
        #[cfg(feature = "event-dispatcher")]
        let mut state = self.state.lock().await;
        #[cfg(feature = "event-dispatcher")]
        if let Some(dispatcher) = state.dispatcher.take() {
            dispatcher.shutdown().await;
        }
        self.engine.update_sink_status(Vec::new());
    }

    async fn install_target(
        &self,
        state: &mut ApplicationServiceState,
        target: Arc<RuntimeConfig>,
    ) -> Result<ConfigReconcileResult, String> {
        if state.installed
            && state.config.as_ref() == target.as_ref()
            && self.services_match_target(state, &target)
        {
            return Ok(ConfigReconcileResult::default());
        }

        let previous = state.config.clone();
        let components = changed_components(previous.as_ref(), target.as_ref());
        let rebuild_hooks = !state.installed || previous.api.hooks != target.api.hooks;
        #[cfg(feature = "event-dispatcher")]
        let rebuild_dispatcher = !state.installed
            || dispatcher_changed(&previous, &target)
            || !self.services_match_target(state, &target);
        #[cfg(feature = "event-dispatcher")]
        if rebuild_dispatcher {
            self.stop_active_services(state).await?;
        }
        state.config = target.clone();
        state.installed = false;

        if rebuild_hooks {
            let warning_handler = {
                let engine = self.engine.clone();
                Some(Arc::new(move |code: &str, message: &str| {
                    engine.emit_warning(code, message);
                }) as Arc<dyn Fn(&str, &str) + Send + Sync>)
            };
            let hook_chain = hooks::build_hook_chain(
                self.ipc_hook_socket.as_deref(),
                &target.api,
                warning_handler,
            );
            self.engine
                .replace_flow_hook_chain((!hook_chain.is_empty()).then_some(hook_chain));
        }

        #[cfg(feature = "event-dispatcher")]
        if rebuild_dispatcher {
            let has_delivery_sinks = !target.api.event_sinks.is_empty();
            let bootstrap_engine_started = has_delivery_sinks && !state.engine_started_bootstrapped;
            state.dispatcher = if bootstrap_engine_started {
                zero_connector::spawn_event_dispatcher_with_engine_started_and_network(
                    self.engine.clone(),
                    target.api.clone(),
                    target.source_dir.clone(),
                    zero_connector::EventDispatcherOptions::default(),
                    self.event_network.clone(),
                )
            } else if let Some(continuation) = &state.continuation {
                zero_connector::resume_event_dispatcher_with_network(
                    self.engine.clone(),
                    target.api.clone(),
                    target.source_dir.clone(),
                    zero_connector::EventDispatcherOptions::default(),
                    self.event_network.clone(),
                    continuation.clone(),
                )
            } else {
                zero_connector::spawn_event_dispatcher_with_network(
                    self.engine.clone(),
                    target.api.clone(),
                    target.source_dir.clone(),
                    zero_connector::EventDispatcherOptions::default(),
                    self.event_network.clone(),
                )
            }
            .map_err(|error| error.to_string())?;
            if state.dispatcher.is_some() {
                state.continuation = None;
            }
            if bootstrap_engine_started && state.dispatcher.is_some() {
                state.engine_started_bootstrapped = true;
            }
            state.status_monitor = Some(spawn_status_monitor(
                self.engine.clone(),
                state.dispatcher.as_ref(),
            ));
        }

        state.installed = true;
        Ok(ConfigReconcileResult { components })
    }

    fn services_match_target(
        &self,
        _state: &ApplicationServiceState,
        _target: &RuntimeConfig,
    ) -> bool {
        #[cfg(feature = "event-dispatcher")]
        if _state.dispatcher.is_some() != dispatcher_enabled(_target) {
            return false;
        }
        true
    }

    #[cfg(feature = "event-dispatcher")]
    async fn stop_active_services(
        &self,
        _state: &mut ApplicationServiceState,
    ) -> Result<(), String> {
        #[cfg(feature = "event-dispatcher")]
        if let Some(monitor) = _state.status_monitor.take() {
            monitor.abort();
        }
        #[cfg(feature = "event-dispatcher")]
        if let Some(dispatcher) = _state.dispatcher.take() {
            _state.installed = false;
            _state.continuation = Some(
                dispatcher
                    .shutdown_for_reload()
                    .await
                    .map_err(|error| error.to_string())?,
            );
        }
        self.engine.update_sink_status(Vec::new());
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConfigApplyReconciler for ApplicationServices {
    fn validate(&self, current: &RuntimeConfig, candidate: &RuntimeConfig) -> Result<(), String> {
        if current.api.control != candidate.api.control {
            return Err(
                "`api.control` cannot be changed by live config.apply; keep the active control endpoint and restart explicitly to replace its listener or credentials"
                    .to_owned(),
            );
        }

        #[cfg(not(feature = "event-dispatcher"))]
        if dispatcher_enabled(candidate) {
            return Err(
                "`api.event_sinks`, `api.dead_letter_path`, and `api.outbox_path` require Cargo feature `event-dispatcher`"
                    .to_owned(),
            );
        }

        Ok(())
    }

    async fn reconcile(&self, target: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        let mut state = self.state.lock().await;
        self.install_target(&mut state, target).await
    }
}

#[cfg(all(test, feature = "sink-jsonl"))]
mod tests;
