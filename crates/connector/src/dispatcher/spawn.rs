use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};

use zero_api::EventSource;
use zero_config::ApiConfig;

use super::*;

mod init;

pub fn spawn_event_dispatcher<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    spawn_event_dispatcher_inner(
        source,
        api,
        source_dir,
        options,
        EventDispatcherNetwork::system(),
        false,
        None,
    )
}

/// Start an event dispatcher with an explicitly supplied network boundary.
///
/// The Zero application uses this entrypoint to share the same physical-egress
/// authority as proxy and DNS sockets. Standalone embeddings that cannot run a
/// TUN data plane may continue to use [`spawn_event_dispatcher`].
pub fn spawn_event_dispatcher_with_network<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    spawn_event_dispatcher_inner(source, api, source_dir, options, network, false, None)
}

/// Start an event dispatcher and bootstrap the retained `engine.started`
/// lifecycle fact after the live subscription has been registered.
pub fn spawn_event_dispatcher_with_engine_started<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    spawn_event_dispatcher_inner(
        source,
        api,
        source_dir,
        options,
        EventDispatcherNetwork::system(),
        true,
        None,
    )
}

/// Start an explicitly network-bound event dispatcher and bootstrap the
/// retained `engine.started` fact.
pub fn spawn_event_dispatcher_with_engine_started_and_network<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    spawn_event_dispatcher_inner(source, api, source_dir, options, network, true, None)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::dispatcher) fn spawn_event_dispatcher_inner<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
    bootstrap_engine_started: bool,
    continuation: Option<EventDispatcherContinuation>,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    let mut handle = prepare_event_dispatcher_inner(
        source::Source::standalone(source),
        api,
        source_dir,
        options,
        network,
        bootstrap_engine_started,
        continuation,
        false,
    );
    if handle.wait_ready_sync()? {
        Ok(Some(handle))
    } else {
        Ok(None)
    }
}

/// Install this original owner before awaiting readiness. Delivery stays paused
/// until `activate()` after the accepted host operation commits.
#[allow(clippy::too_many_arguments)]
pub fn prepare_event_dispatcher_paused_with_network<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
    bootstrap_engine_started: bool,
    continuation: Option<EventDispatcherContinuation>,
) -> EventDispatcherHandle
where
    S: EventSource + Send + Sync + 'static,
{
    prepare_event_dispatcher_inner(
        source::Source::standalone(source),
        api,
        source_dir,
        options,
        network,
        bootstrap_engine_started,
        continuation,
        true,
    )
}

/// Prepare delivery from the same concrete Engine and immutable event originals.
/// Persistent sinks consume a registration cursor, not a second flow snapshot.
#[cfg(feature = "kernel-events")]
#[allow(clippy::too_many_arguments)]
pub fn prepare_kernel_event_dispatcher_paused_with_network(
    engine: zero_engine::EngineHandle,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
    bootstrap_engine_started: bool,
    continuation: Option<EventDispatcherContinuation>,
) -> EventDispatcherHandle {
    prepare_event_dispatcher_inner(
        source::Source::kernel(engine),
        api,
        source_dir,
        options,
        network,
        bootstrap_engine_started,
        continuation,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_event_dispatcher_inner<S>(
    source: source::Source<S>,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
    bootstrap_engine_started: bool,
    continuation: Option<EventDispatcherContinuation>,
    paused: bool,
) -> EventDispatcherHandle
where
    S: EventSource + Send + Sync + 'static,
{
    let (init_tx, init_rx) = mpsc::sync_channel(1);
    let init_notify = Arc::new(tokio::sync::Notify::new());
    let init_tx = InitSignal::new(init_tx, init_notify.clone());
    let (activate_tx, activate_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let sink_stats: SharedSinkStats = Arc::new(Mutex::new(Vec::new()));
    let stats_for_handle = sink_stats.clone();
    let stats_for_thread = sink_stats.clone();
    let capacity = Arc::new(crate::capacity::DispatcherCapacity::from_api(&api));
    let children = Arc::new(DispatcherChildren::with_capacity(capacity.clone()));
    let worker_children = children.clone();
    let resources = DispatcherResources::with_capacity(capacity.clone());
    let worker_resources = resources.clone();

    let prepared =
        crate::capacity::DispatcherCapacity::validate(&api).and_then(|()| capacity.task());
    let task_charge = match prepared {
        Ok(charge) => charge,
        Err(error) => {
            return EventDispatcherHandle {
                shutdown: None,
                task: None,
                task_charge: None,
                task_exit: Some(Ok(None)),
                children,
                resources,
                init_rx: None,
                init_notify,
                init_result: Some(Err(Arc::new(ConnectorError::Config(error.to_string())))),
                activate: None,
                sink_stats: stats_for_handle,
            };
        }
    };
    let (start_initializer, initializer_ready) = mpsc::sync_channel(1);
    let task = init::spawn_initializer_thread(
        initializer_ready,
        source,
        api,
        source_dir,
        options,
        network,
        bootstrap_engine_started,
        continuation,
        paused.then_some(activate_rx),
        shutdown_rx,
        init_tx,
        stats_for_thread,
        worker_resources,
        worker_children,
    );

    let handle = EventDispatcherHandle {
        shutdown: Some(shutdown_tx),
        task: Some(task),
        task_charge: Some(task_charge),
        task_exit: None,
        children,
        resources,
        init_rx: Some(init_rx),
        init_notify,
        init_result: None,
        activate: paused.then_some(activate_tx),
        sink_stats: stats_for_handle,
    };
    let _ = start_initializer.send(());
    handle
}
