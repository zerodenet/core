use std::path::PathBuf;
use std::sync::{mpsc, Arc};

use tokio::task::JoinHandle;
use tracing::debug;
use zero_api::EventSource;
use zero_config::ApiConfig;

use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_initializer_thread<S>(
    initializer_ready: mpsc::Receiver<()>,
    source: source::Source<S>,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
    bootstrap_engine_started: bool,
    continuation: Option<EventDispatcherContinuation>,
    activate_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    shutdown_rx: tokio::sync::oneshot::Receiver<ShutdownMode>,
    init_tx: InitSignal,
    stats_for_thread: SharedSinkStats,
    worker_resources: DispatcherResources,
    worker_children: Arc<DispatcherChildren>,
) -> JoinHandle<Option<EventDispatcherContinuation>>
where
    S: EventSource + Send + Sync + 'static,
{
    let dead_letter_path = api.dead_letter_path.clone();
    let outbox_path = api.outbox_path.clone();

    tokio::task::spawn_blocking(move || {
        if initializer_ready.recv().is_err() {
            return None;
        }
        let core_instance_id = match source.core_instance_id() {
            Ok(identity) => identity,
            Err(error) => {
                let _ = init_tx.send(Err(ConnectorError::Api(error)));
                return None;
            }
        };
        if let Some(state) = &continuation {
            if let Err(error) = state.validate(&core_instance_id) {
                let _ = init_tx.send(Err(error));
                return None;
            }
        }
        let sinks = match build_event_sinks(
            &api,
            source_dir.as_deref(),
            &network,
            &worker_resources,
            worker_children.clone(),
        ) {
            Ok(sinks) => sinks,
            Err(error) => {
                let _ = init_tx.send(Err(error));
                return None;
            }
        };

        if sinks.is_empty() && dead_letter_path.is_none() {
            let _ = init_tx.send(Ok(false));
            return None;
        }

        {
            let mut shared = stats_for_thread.lock().expect("sink stats");
            for sink in &sinks {
                shared.push((sink.tag.clone(), Arc::new(PerSinkStats::new())));
            }
        }

        let (dead_letter, _dead_letter_lease) = match dead_letter_path {
            Some(path) => {
                let path = resolve_path(&path, source_dir.as_deref());
                let lease =
                    match crate::state::PersistentStateLease::acquire(&path, &worker_resources) {
                        Ok(lease) => lease,
                        Err(error) => {
                            let _ = init_tx.send(Err(error));
                            return None;
                        }
                    };
                let sink = match RetainedDeadLetter::new(&path, &worker_resources) {
                    Ok(sink) => sink,
                    Err(error) => {
                        let _ = init_tx.send(Err(error.into()));
                        return None;
                    }
                };
                debug!(path = %path.display(), "dead-letter sink enabled");
                (Some(sink), Some(lease))
            }
            None => (None, None),
        };

        let dispatcher_config = api.dispatcher;
        let mut outbox = match outbox_path {
            Some(path) => match DeliveryOutbox::open_owned(
                worker_resources.clone(),
                &path,
                source_dir.as_deref(),
                dispatcher_config.outbox_min_free_bytes,
                dispatcher_config.outbox_min_free_percent,
            ) {
                Ok(outbox) => Some(outbox),
                Err(error) => {
                    let _ = init_tx.send(Err(error));
                    return None;
                }
            },
            None => None,
        };

        let startup_sequence = match source.startup_sequence() {
            Ok(sequence) => sequence,
            Err(error) => {
                let _ = init_tx.send(Err(ConnectorError::Api(error)));
                return None;
            }
        };
        let (subscriber, baseline) = match source.subscribe() {
            Ok(subscription) => subscription,
            Err(error) => {
                let _ = init_tx.send(Err(ConnectorError::Api(error)));
                return None;
            }
        };
        let source_budget = source::SourceBudget::new(
            dispatcher_config.replay_batch_size,
            dispatcher_config.source_queue_max_bytes,
        );
        let bootstrap_events = if bootstrap_engine_started {
            match source.bootstrap(&source_budget) {
                Ok(event) => event.into_iter().collect(),
                Err(error) => {
                    let _ = init_tx.send(Err(ConnectorError::Api(error)));
                    return None;
                }
            }
        } else {
            Vec::new()
        };
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = init_tx.send(Err(ConnectorError::DispatcherRuntime(error)));
                return None;
            }
        };
        let continuation = match continuation.map(|state| state.take()).transpose() {
            Ok(state) => state,
            Err(error) => {
                let _ = init_tx.send(Err(error));
                return None;
            }
        };
        Some(runtime.block_on(run_event_dispatcher(
            source,
            subscriber,
            baseline,
            source_budget,
            bootstrap_events,
            sinks,
            &stats_for_thread,
            options,
            shutdown_rx,
            dead_letter,
            &mut outbox,
            dispatcher_config,
            core_instance_id,
            startup_sequence,
            continuation,
            init_tx,
            activate_rx,
            worker_children,
        )))
    })
}
