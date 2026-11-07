use std::error::Error;
use std::sync::Arc;

use tokio::sync::Mutex;
use zero_config::RuntimeConfig;
use zero_engine::Engine;
use zero_proxy::{ConfigApplyReconciler, ConfigReconcileResult};

use crate::hooks;

mod cleanup_fault;
use cleanup_fault::{CleanupOwner, CleanupReceivers};
mod control;
mod startup;
use startup::{ApplicationStartupSlot, APPLICATION_STARTUP};
mod dispatcher;
use dispatcher::{changed_components, dispatcher_enabled};
#[cfg(feature = "event-dispatcher")]
use dispatcher::{dispatcher_changed, start_status_monitor, StatusMonitor};

pub(crate) mod network;

#[cfg(feature = "event-dispatcher")]
use network::ApplicationEventSinkTcpDialer;

pub(super) struct ApplicationServices {
    engine: Engine,
    ipc_hook_socket: Option<String>,
    #[cfg(feature = "event-dispatcher")]
    event_network: zero_connector::EventDispatcherNetwork,
    state: Mutex<ApplicationServiceState>,
    #[cfg(feature = "event-dispatcher")]
    monitor_changed: tokio::sync::Notify,
    control: std::sync::Mutex<control::ControlServers>,
    parent_input: std::sync::OnceLock<Arc<zero_platform_tokio::parent_input::ParentInputOwner>>,
    control_budget: Arc<zero_runtime::configuration::ControlBudget>,
    embedded_commands: Arc<zero_runtime::EmbeddedCommandOwner>,
    proxy_control_budget: bool,
}

struct ApplicationAttempt {
    target: Arc<RuntimeConfig>,
    dispatcher_touched: bool,
    hooks_touched: bool,
    control_budget: Option<zero_runtime::configuration::ControlBudgetCandidate>,
    #[cfg(feature = "event-dispatcher")]
    reported_dispatcher_preparation: Option<Arc<zero_connector::ConnectorError>>,
}
struct ApplicationServiceState {
    attempt: Option<ApplicationAttempt>,
    #[cfg(test)]
    preparation_pause: Option<Arc<startup::StartupPause>>,
    pending_hooks: Option<hooks::PreparedHooks>,
    hooks: Option<hooks::PreparedHooks>,
    cleanup_fault: Option<Arc<dyn Error + Send + Sync>>,
    cleanup_receivers: CleanupReceivers,
    #[cfg(feature = "event-dispatcher")]
    monitor_exit: Option<Result<(), Arc<zero_platform_tokio::contract::ChannelError>>>,
    #[cfg(feature = "event-dispatcher")]
    network_generation_started: bool,
    config: Arc<RuntimeConfig>,
    installed: bool,
    #[cfg(feature = "event-dispatcher")]
    dispatcher: Option<zero_connector::EventDispatcherHandle>,
    #[cfg(feature = "event-dispatcher")]
    continuation: Option<zero_connector::EventDispatcherContinuation>,
    #[cfg(feature = "event-dispatcher")]
    status_monitor: Option<Arc<StatusMonitor>>,
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
        let event_network = {
            // Explicit standalone test fixture: a real catalog issues the
            // identity and its actual root is retained by the tested owner.
            let preparation = zero_proxy::ProxyPreparation::new();
            preparation.configure_original_limit(&engine.config().runtime)?;
            let (instance, originals) = preparation.application_network_originals()?;
            zero_connector::EventDispatcherNetwork::new(Arc::new(
                ApplicationEventSinkTcpDialer::new(
                    preparation.egress_interface_control(),
                    instance,
                    originals,
                ),
            ))
        };
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
        let event_network = {
            let (instance, originals) = proxy.application_network_originals()?;
            zero_connector::EventDispatcherNetwork::new(Arc::new(
                ApplicationEventSinkTcpDialer::new(
                    proxy.egress_interface_control(),
                    instance,
                    originals,
                ),
            ))
        };
        Self::start_with_network_owned(
            engine,
            ipc_hook_socket,
            #[cfg(feature = "event-dispatcher")]
            event_network,
            &APPLICATION_STARTUP,
            Some((
                proxy.application_control_budget(),
                proxy.embedded_command_owner(),
            )),
        )
        .await
    }

    async fn start_with_network(
        engine: Engine,
        ipc_hook_socket: Option<&str>,
        #[cfg(feature = "event-dispatcher")] event_network: zero_connector::EventDispatcherNetwork,
        startup: &ApplicationStartupSlot,
    ) -> Result<Arc<Self>, Box<dyn Error>> {
        Self::start_with_network_owned(
            engine,
            ipc_hook_socket,
            #[cfg(feature = "event-dispatcher")]
            event_network,
            startup,
            None,
        )
        .await
    }
    async fn start_with_network_owned(
        engine: Engine,
        ipc_hook_socket: Option<&str>,
        #[cfg(feature = "event-dispatcher")] event_network: zero_connector::EventDispatcherNetwork,
        startup: &ApplicationStartupSlot,
        owned: Option<(
            Arc<zero_runtime::configuration::ControlBudget>,
            Arc<zero_runtime::EmbeddedCommandOwner>,
        )>,
    ) -> Result<Arc<Self>, Box<dyn Error>> {
        let config = engine.config();
        let proxy_control_budget = owned.is_some();
        let (control_budget, embedded_commands) = match owned {
            Some((budget, commands)) => {
                assert!(
                    Arc::ptr_eq(&commands, &budget.command_owner()),
                    "same actual Host control owner"
                );
                (budget, commands)
            }
            None => {
                let budget = zero_runtime::configuration::ControlBudget::new(
                    zero_runtime::configuration::ControlBudgetLimits::from_runtime(&config.runtime),
                );
                let commands = budget.command_owner();
                (budget, commands)
            }
        };
        let services = Arc::new(Self {
            engine,
            #[cfg(feature = "event-dispatcher")]
            monitor_changed: tokio::sync::Notify::new(),
            ipc_hook_socket: ipc_hook_socket.map(str::to_owned),
            #[cfg(feature = "event-dispatcher")]
            event_network,
            control: std::sync::Mutex::new(control::ControlServers::default()),
            parent_input: std::sync::OnceLock::new(),
            control_budget,
            embedded_commands,
            proxy_control_budget,
            state: Mutex::new(ApplicationServiceState {
                config: config.clone(),
                installed: false,
                attempt: None,
                #[cfg(test)]
                preparation_pause: startup.pause.lock().unwrap().clone(),
                pending_hooks: None,
                hooks: None,
                cleanup_fault: None,
                cleanup_receivers: CleanupReceivers::default(),
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
        // Only this actual startup slot may reopen after its old native servers
        // and workers have positively retired. A failed reserve never reopens.
        if let Err(error) = services
            .embedded_commands
            .reopen_after_retirement()
            .and_then(|_| {
                services.control_budget.reopen_for_runtime(
                    zero_runtime::configuration::ControlBudgetLimits::from_runtime(&config.runtime),
                )
            })
        {
            startup.clear(&services);
            return Err(error.into());
        }
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
