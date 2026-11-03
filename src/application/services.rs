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

struct ApplicationAttempt {
    target: Arc<RuntimeConfig>,
    dispatcher_touched: bool,
}
struct ApplicationServiceState {
    attempt: Option<ApplicationAttempt>,
    pending_hooks: Option<Option<zero_engine::FlowHookChain>>,
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
                attempt: None,
                pending_hooks: None,
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

        services
            .validate(config.as_ref(), config.as_ref())
            .map_err(std::io::Error::other)?;
        {
            let mut state = services.state.lock().await;
            if let Err(error) = services.install_target(&mut state, config).await {
                #[cfg(feature = "event-dispatcher")]
                let cleanup = services.stop_active_services(&mut state).await;
                #[cfg(not(feature = "event-dispatcher"))]
                let cleanup: Result<(), String> = Ok(());
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
        Ok(services)
    }
}

mod configuration;
mod installation;
mod lifecycle;

#[cfg(all(test, feature = "sink-jsonl"))]
mod tests;
