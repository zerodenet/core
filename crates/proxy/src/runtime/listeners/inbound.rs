use std::collections::HashMap;
use std::path::Path;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{info, warn};
use zero_config::{InboundConfig, RuntimeConfig};
use zero_engine::EngineError;

use crate::inventory::ProtocolInventory;
use crate::runtime::route_runtime::InboundListenerRuntimeFactory;

pub(in crate::runtime) struct InboundReconcileState<'a> {
    pub listener_stops: &'a mut HashMap<String, watch::Sender<bool>>,
    pub active_inbounds: &'a mut HashMap<String, InboundConfig>,
    pub expected_listener_exits: &'a mut usize,
    pub listeners: &'a mut JoinSet<Result<(), EngineError>>,
}

pub(in crate::runtime) async fn bind_inbound_listener(
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    inbound: &InboundConfig,
) -> Result<crate::protocol_registry::BoundInbound, EngineError> {
    protocols.bind_inbound(inbound, source_dir).await
}

pub(in crate::runtime) fn spawn_inbound_listener(
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    runtime_factory: &InboundListenerRuntimeFactory,
    inbound: &InboundConfig,
    bound: crate::protocol_registry::BoundInbound,
    shutdown_rx: watch::Receiver<bool>,
    listeners: &mut JoinSet<Result<(), EngineError>>,
) -> Result<(), EngineError> {
    let operation = protocols
        .prepare_inbound_listener(inbound.clone(), source_dir)
        .map_err(|error| {
            warn!(tag = %inbound.tag, error = %error, "inbound listener adapter preparation failed");
            error
        })?;
    listeners.spawn(operation.execute(
        runtime_factory.for_inbound(inbound.tag.clone()),
        bound,
        shutdown_rx,
    ));
    Ok(())
}

pub(in crate::runtime) async fn reconcile_inbounds(
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    runtime_factory: &InboundListenerRuntimeFactory,
    rollback_runtime_factory: &InboundListenerRuntimeFactory,
    new_config: &RuntimeConfig,
    state: InboundReconcileState<'_>,
) -> Result<(), EngineError> {
    let new_tags: Vec<&str> = new_config
        .inbounds
        .iter()
        .map(|item| item.tag.as_str())
        .collect();

    state.listener_stops.retain(|tag, shutdown| {
        if new_tags.contains(&tag.as_str()) {
            true
        } else {
            let _ = shutdown.send(true);
            *state.expected_listener_exits = state.expected_listener_exits.saturating_add(1);
            info!(%tag, "signalled shutdown for removed inbound listener");
            false
        }
    });
    state
        .active_inbounds
        .retain(|tag, _| new_tags.contains(&tag.as_str()));

    for inbound in &new_config.inbounds {
        let previous = state.active_inbounds.get(&inbound.tag).cloned();
        if previous
            .as_ref()
            .is_some_and(|current| !requires_listener_restart(current, inbound))
        {
            state
                .active_inbounds
                .insert(inbound.tag.clone(), inbound.clone());
            continue;
        }

        if let Some(shutdown) = state.listener_stops.remove(&inbound.tag) {
            let _ = shutdown.send(true);
            *state.expected_listener_exits = state.expected_listener_exits.saturating_add(1);
            info!(tag = %inbound.tag, "signalled shutdown for changed inbound listener");
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let error = match bind_inbound_with_retry(protocols, source_dir, inbound).await {
            Ok(bound) => match spawn_inbound_listener(
                protocols,
                source_dir,
                runtime_factory,
                inbound,
                bound,
                shutdown_rx,
                state.listeners,
            ) {
                Ok(()) => {
                    state
                        .listener_stops
                        .insert(inbound.tag.clone(), shutdown_tx);
                    state
                        .active_inbounds
                        .insert(inbound.tag.clone(), inbound.clone());
                    info!(tag = %inbound.tag, "started new inbound listener");
                    continue;
                }
                Err(error) => {
                    warn!(tag = %inbound.tag, error = %error, "failed to prepare inbound listener");
                    error
                }
            },
            Err(error) => {
                warn!(tag = %inbound.tag, error = %error, "failed to bind inbound listener");
                error
            }
        };
        if let Some(previous) = previous {
            let (rollback_tx, rollback_rx) = watch::channel(false);
            match bind_inbound_with_retry(protocols, source_dir, &previous).await {
                Ok(bound) => match spawn_inbound_listener(
                    protocols,
                    source_dir,
                    rollback_runtime_factory,
                    &previous,
                    bound,
                    rollback_rx,
                    state.listeners,
                ) {
                    Ok(()) => {
                        state
                            .listener_stops
                            .insert(previous.tag.clone(), rollback_tx);
                        state.active_inbounds.insert(previous.tag.clone(), previous);
                    }
                    Err(rollback_error) => {
                        warn!(tag = %inbound.tag, %rollback_error, "failed to prepare previous inbound during rollback");
                    }
                },
                Err(rollback_error) => {
                    warn!(tag = %inbound.tag, %rollback_error, "failed to rebind previous inbound during rollback");
                }
            }
        }
        return Err(error);
    }
    Ok(())
}

async fn bind_inbound_with_retry(
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    inbound: &InboundConfig,
) -> Result<crate::protocol_registry::BoundInbound, EngineError> {
    let mut last_error = None;
    for attempt in 0..50 {
        match bind_inbound_listener(protocols, source_dir, inbound).await {
            Ok(bound) => return Ok(bound),
            Err(error) => last_error = Some(error),
        }
        if attempt < 49 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    Err(last_error.expect("bind retry loop always attempts at least once"))
}

fn requires_listener_restart(current: &InboundConfig, next: &InboundConfig) -> bool {
    let mut current = current.clone();
    let mut next = next.clone();
    clear_live_managed_credentials(&mut current.protocol);
    clear_live_managed_credentials(&mut next.protocol);
    current != next
}

fn clear_live_managed_credentials(protocol: &mut zero_config::InboundProtocolConfig) {
    use zero_config::InboundProtocolConfig;
    match protocol {
        InboundProtocolConfig::Vless { users, .. } => users.clear(),
        InboundProtocolConfig::Vmess { users, .. } => users.clear(),
        InboundProtocolConfig::Trojan {
            password, users, ..
        } => {
            password.clear();
            users.clear();
        }
        InboundProtocolConfig::Shadowsocks {
            password, users, ..
        } => {
            password.clear();
            users.clear();
        }
        InboundProtocolConfig::Hysteria2 {
            password, users, ..
        } => {
            password.clear();
            users.clear();
        }
        _ => {}
    }
}
