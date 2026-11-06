use std::error::Error;
use std::sync::Arc;

use tokio::sync::Mutex;
use zero_config::RuntimeConfig;
use zero_engine::Engine;
use zero_proxy::{ConfigApplyReconciler, ConfigReconcileResult};

use crate::hooks;

mod control;
mod startup;
use startup::{ApplicationStartupSlot, APPLICATION_STARTUP};
mod dispatcher;
use dispatcher::{changed_components, dispatcher_enabled};
#[cfg(feature = "event-dispatcher")]
use dispatcher::{dispatcher_changed, spawn_status_monitor};

pub(crate) mod network;

#[cfg(feature = "event-dispatcher")]
use network::ApplicationEventSinkTcpDialer;

pub(super) struct ApplicationServices {
    engine: Engine,
    ipc_hook_socket: Option<String>,
    #[cfg(feature = "event-dispatcher")]
    event_network: zero_connector::EventDispatcherNetwork,
    state: Mutex<ApplicationServiceState>,
    control: std::sync::Mutex<control::ControlServers>,
    parent_input: std::sync::OnceLock<Arc<zero_platform_tokio::parent_input::ParentInputOwner>>,
    control_budget: Arc<zero_runtime::configuration::ControlBudget>,
}

struct ApplicationAttempt {
    target: Arc<RuntimeConfig>,
    dispatcher_touched: bool,
    hooks_touched: bool,
    control_budget: Option<zero_runtime::configuration::ControlBudgetCandidate>,
}
struct ApplicationServiceState {
    attempt: Option<ApplicationAttempt>,
    #[cfg(test)]
    preparation_pause: Option<Arc<startup::StartupPause>>,
    pending_hooks: Option<hooks::PreparedHooks>,
    hooks: Option<hooks::PreparedHooks>,
    cleanup_fault: Option<Arc<dyn Error + Send + Sync>>,
    #[cfg(feature = "event-dispatcher")]
    monitor_exit: Option<Result<(), Arc<tokio::task::JoinError>>>,
    #[cfg(feature = "event-dispatcher")]
    network_generation_started: bool,
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
        let event_network = zero_connector::EventDispatcherNetwork::new(Arc::new(
            ApplicationEventSinkTcpDialer::new(
                zero_platform_tokio::EgressInterfaceControl::default(),
            ),
        ));
        let slot = ApplicationStartupSlot::new();
        Self::start_with_network(
            engine,
            ipc_hook_socket,
            #[cfg(feature = "event-dispatcher")]
            event_network,
            &slot,
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
            &APPLICATION_STARTUP,
        )
        .await
    }

    async fn start_with_network(
        engine: Engine,
        ipc_hook_socket: Option<&str>,
        #[cfg(feature = "event-dispatcher")] event_network: zero_connector::EventDispatcherNetwork,
        startup: &ApplicationStartupSlot,
    ) -> Result<Arc<Self>, Box<dyn Error>> {
        let config = engine.config();
        let services = Arc::new(Self {
            engine,
            ipc_hook_socket: ipc_hook_socket.map(str::to_owned),
            #[cfg(feature = "event-dispatcher")]
            event_network,
            control: std::sync::Mutex::new(control::ControlServers::default()),
            parent_input: std::sync::OnceLock::new(),
            control_budget: zero_runtime::configuration::ControlBudget::new(
                zero_runtime::configuration::ControlBudgetLimits::from_runtime(&config.runtime),
            ),
            state: Mutex::new(ApplicationServiceState {
                config: config.clone(),
                installed: false,
                attempt: None,
                #[cfg(test)]
                preparation_pause: startup.pause.lock().unwrap().clone(),
                pending_hooks: None,
                hooks: None,
                cleanup_fault: None,
                #[cfg(feature = "event-dispatcher")]
                monitor_exit: None,
                #[cfg(feature = "event-dispatcher")]
                network_generation_started: false,
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

        // This exact original is installed before any cancellable await.
        startup.install(services.clone())?;
        if let Err(error) = services.validate(config.as_ref(), config.as_ref()) {
            // Validation is pure and no service resource exists yet.
            startup.clear(&services);
            return Err(std::io::Error::other(error).into());
        }
        {
            let mut state = services.state.lock().await;
            if let Err(error) = services.install_target(&mut state, config).await {
                #[cfg(feature = "event-dispatcher")]
                let cleanup = services.stop_active_services(&mut state).await;
                #[cfg(not(feature = "event-dispatcher"))]
                let cleanup: Result<(), String> = Ok(());
                let hooks_cleanup = services.retire_hooks(&mut state);
                let cleanup = cleanup.and(hooks_cleanup);
                if cleanup.is_ok() {
                    startup.clear(&services);
                }
                return Err(std::io::Error::other(format!(
                    "{error}; original startup cleanup: {}",
                    cleanup.err().unwrap_or_else(|| "confirmed".into())
                ))
                .into());
            }
            services.publish_prepared_hooks(&mut state);
            #[cfg(feature = "event-dispatcher")]
            if let Some(dispatcher) = state.dispatcher.as_mut() {
                dispatcher.activate().map_err(std::io::Error::other)?;
            }
        }
        // No await between the real Arc handoff and clearing the startup slot.
        startup.clear(&services);
        Ok(services)
    }
}

mod configuration;
mod installation;
mod lifecycle;

#[cfg(all(test, feature = "sink-jsonl"))]
mod tests;
