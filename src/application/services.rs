use std::error::Error;
use std::sync::Arc;

use tokio::sync::Mutex;
use zero_config::RuntimeConfig;
use zero_engine::Engine;
use zero_proxy::{ConfigApplyReconciler, ConfigReconcileResult};

use crate::hooks;

pub(super) struct ApplicationServices {
    engine: Engine,
    ipc_hook_socket: Option<String>,
    state: Mutex<ApplicationServiceState>,
}

struct ApplicationServiceState {
    config: Arc<RuntimeConfig>,
    installed: bool,
    #[cfg(feature = "event-dispatcher")]
    dispatcher: Option<zero_connector::EventDispatcherHandle>,
    #[cfg(feature = "event-dispatcher")]
    status_monitor: Option<tokio::task::JoinHandle<()>>,
    #[cfg(feature = "event-dispatcher")]
    engine_started_bootstrapped: bool,
}

impl ApplicationServices {
    pub(super) async fn start(
        engine: Engine,
        ipc_hook_socket: Option<&str>,
    ) -> Result<Arc<Self>, Box<dyn Error>> {
        let config = engine.config();
        let services = Arc::new(Self {
            engine,
            ipc_hook_socket: ipc_hook_socket.map(str::to_owned),
            state: Mutex::new(ApplicationServiceState {
                config: config.clone(),
                installed: false,
                #[cfg(feature = "event-dispatcher")]
                dispatcher: None,
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

        self.stop_active_services(state).await;
        state.config = target.clone();
        state.installed = false;

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

        #[cfg(feature = "event-dispatcher")]
        {
            let has_delivery_sinks = !target.api.event_sinks.is_empty();
            let bootstrap_engine_started = has_delivery_sinks && !state.engine_started_bootstrapped;
            state.dispatcher = if bootstrap_engine_started {
                zero_connector::spawn_event_dispatcher_with_engine_started(
                    self.engine.clone(),
                    target.api.clone(),
                    target.source_dir.clone(),
                    zero_connector::EventDispatcherOptions::default(),
                )
            } else {
                zero_connector::spawn_event_dispatcher(
                    self.engine.clone(),
                    target.api.clone(),
                    target.source_dir.clone(),
                    zero_connector::EventDispatcherOptions::default(),
                )
            }
            .map_err(|error| error.to_string())?;
            if bootstrap_engine_started && state.dispatcher.is_some() {
                state.engine_started_bootstrapped = true;
            }
        }

        #[cfg(feature = "event-dispatcher")]
        {
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

    async fn stop_active_services(&self, _state: &mut ApplicationServiceState) {
        #[cfg(feature = "event-dispatcher")]
        if let Some(monitor) = _state.status_monitor.take() {
            monitor.abort();
        }
        #[cfg(feature = "event-dispatcher")]
        if let Some(dispatcher) = _state.dispatcher.take() {
            dispatcher.shutdown().await;
        }
        self.engine.update_sink_status(Vec::new());
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

fn dispatcher_enabled(config: &RuntimeConfig) -> bool {
    !config.api.event_sinks.is_empty()
        || config.api.dead_letter_path.is_some()
        || config.api.outbox_path.is_some()
}

fn changed_components(previous: &RuntimeConfig, target: &RuntimeConfig) -> Vec<String> {
    let mut components = Vec::new();
    if previous.api.hooks != target.api.hooks {
        components.push("flow-hooks".to_owned());
    }
    if previous.api.event_sinks != target.api.event_sinks
        || previous.api.dead_letter_path != target.api.dead_letter_path
        || previous.api.outbox_path != target.api.outbox_path
        || previous.api.dispatcher != target.api.dispatcher
    {
        components.push("event-dispatcher".to_owned());
    }
    components
}

#[cfg(feature = "event-dispatcher")]
fn spawn_status_monitor(
    engine: Engine,
    dispatcher: Option<&zero_connector::EventDispatcherHandle>,
) -> tokio::task::JoinHandle<()> {
    let dispatcher = dispatcher.map(zero_connector::EventDispatcherHandle::status_handle);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let mut status = Vec::new();
            if let Some(dispatcher) = &dispatcher {
                status.extend(dispatcher.sink_status());
            }
            engine.update_sink_status(status);
        }
    })
}

#[cfg(all(test, feature = "sink-jsonl"))]
mod tests;
