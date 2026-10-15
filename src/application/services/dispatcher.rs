//! Application wiring and projection for the optional event dispatcher.
use super::*;

pub(super) fn dispatcher_enabled(config: &RuntimeConfig) -> bool {
    !config.api.event_sinks.is_empty()
        || config.api.dead_letter_path.is_some()
        || config.api.outbox_path.is_some()
}

pub(super) fn changed_components(previous: &RuntimeConfig, target: &RuntimeConfig) -> Vec<String> {
    let mut components = Vec::new();
    if previous.api.hooks != target.api.hooks {
        components.push("flow-hooks".to_owned());
    }
    if dispatcher_changed(previous, target) {
        components.push("event-dispatcher".to_owned());
    }
    components
}

pub(super) fn dispatcher_changed(previous: &RuntimeConfig, target: &RuntimeConfig) -> bool {
    previous.api.event_sinks != target.api.event_sinks
        || previous.api.dead_letter_path != target.api.dead_letter_path
        || previous.api.outbox_path != target.api.outbox_path
        || previous.api.dispatcher != target.api.dispatcher
        || (previous.source_dir != target.source_dir
            && (dispatcher_enabled(previous) || dispatcher_enabled(target)))
}

#[cfg(feature = "event-dispatcher")]
pub(super) fn spawn_status_monitor(
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
