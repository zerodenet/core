use std::collections::HashMap;
use std::path::Path;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use zero_config::{InboundConfig, RuntimeConfig};
use zero_engine::EngineError;

use super::reservation::{bind_inbound_with_retry, reserve_candidates};
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
    runtime_factory: &InboundListenerRuntimeFactory,
    source_dir: Option<&Path>,
    inbound: &InboundConfig,
) -> Result<crate::protocol_registry::BoundInbound, EngineError> {
    protocols
        .bind_inbound(&runtime_factory.listener_config(inbound), source_dir)
        .await
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
    spawn_inbound_listener_with_state(
        protocols,
        source_dir,
        runtime_factory,
        inbound,
        bound,
        shutdown_rx,
        listeners,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn spawn_inbound_listener_with_state(
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    runtime_factory: &InboundListenerRuntimeFactory,
    inbound: &InboundConfig,
    bound: crate::protocol_registry::BoundInbound,
    shutdown_rx: watch::Receiver<bool>,
    listeners: &mut JoinSet<Result<(), EngineError>>,
    rollback: bool,
) -> Result<(), EngineError> {
    let listener_config = runtime_factory.listener_config(inbound);
    let operation = (if rollback {
        protocols.prepare_rollback_inbound_listener(listener_config, source_dir)
    } else {
        protocols.prepare_inbound_listener(listener_config, source_dir)
    })
    .map_err(|error| {
        warn!(
            inbound_tag = %inbound.tag,
            protocol = inbound.protocol.protocol_name(),
            listen_address = %inbound.listen.address,
            listen_port = inbound.listen.port,
            reason = "adapter_prepare_error",
            error = %error,
            "inbound listener adapter preparation failed"
        );
        error
    })?;
    let inbound_tag = inbound.tag.clone();
    let protocol = inbound.protocol.protocol_name();
    let listen_address = inbound.listen.address.clone();
    let listen_port = inbound.listen.port;
    let listener_runtime = runtime_factory.for_inbound(inbound_tag.clone());

    listeners.spawn(async move {
        info!(
            inbound_tag = %inbound_tag,
            protocol = protocol,
            listen_address = %listen_address,
            listen_port = listen_port,
            "inbound listener task started"
        );
        let result = operation
            .execute(listener_runtime, bound, shutdown_rx)
            .await;
        match &result {
            Ok(()) => info!(
                inbound_tag = %inbound_tag,
                protocol = protocol,
                listen_address = %listen_address,
                listen_port = listen_port,
                reason = "listener_task_returned",
                "inbound listener task returned"
            ),
            Err(listener_error) => error!(
                inbound_tag = %inbound_tag,
                protocol = protocol,
                listen_address = %listen_address,
                listen_port = listen_port,
                reason = "listener_task_error",
                error = %listener_error,
                "inbound listener task failed"
            ),
        }
        result
    });
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
    // Reserve every independently bindable candidate before replacing any
    // live listener. A later bind failure must not expose an earlier candidate
    // protocol session or require re-creating its original socket on rollback.
    let mut reserved = reserve_candidates(
        protocols,
        runtime_factory,
        source_dir,
        new_config,
        state.active_inbounds,
    )
    .await?;
    let new_tags: Vec<&str> = new_config
        .inbounds
        .iter()
        .filter(|item| {
            runtime_factory
                .endpoint_admission()
                .listener_enabled(&item.tag)
        })
        .map(|item| item.tag.as_str())
        .collect();

    let mut removed = Vec::new();
    state.listener_stops.retain(|tag, shutdown| {
        if new_tags.contains(&tag.as_str()) {
            true
        } else {
            let _ = shutdown.send(true);
            removed.push(shutdown.clone());
            *state.expected_listener_exits = state.expected_listener_exits.saturating_add(1);
            info!(%tag, reason = "config_removed", "signalled shutdown for removed inbound listener");
            false
        }
    });
    state
        .active_inbounds
        .retain(|tag, _| new_tags.contains(&tag.as_str()));
    for shutdown in removed {
        wait_listener_stopped(&shutdown).await?;
    }

    for inbound in &new_config.inbounds {
        if !runtime_factory
            .endpoint_admission()
            .listener_enabled(&inbound.tag)
        {
            continue;
        }
        let previous = state.active_inbounds.get(&inbound.tag).cloned();
        let mode_changed = previous.is_some()
            && protocols
                .inbound_listener_requires_restart(&runtime_factory.listener_config(inbound))?;
        if previous.as_ref().is_some_and(|current| {
            !requires_listener_restart(
                &rollback_runtime_factory.listener_config(current),
                &runtime_factory.listener_config(inbound),
            )
        }) && !mode_changed
        {
            state
                .active_inbounds
                .insert(inbound.tag.clone(), inbound.clone());
            continue;
        }

        if previous.as_ref().is_some_and(|current| {
            current.listen == inbound.listen
                && current.protocol.protocol_name() == inbound.protocol.protocol_name()
        }) && protocols
            .update_inbound_listener(runtime_factory.listener_config(inbound), source_dir)?
        {
            state
                .active_inbounds
                .insert(inbound.tag.clone(), inbound.clone());
            info!(inbound_tag = %inbound.tag, reason = "listener_updated_in_place", "updated inbound listener on its existing socket");
            continue;
        }

        let prebound = reserved.remove(&inbound.tag);

        if let Some(shutdown) = state.listener_stops.remove(&inbound.tag) {
            let _ = shutdown.send(true);
            wait_listener_stopped(&shutdown).await?;
            *state.expected_listener_exits = state.expected_listener_exits.saturating_add(1);
            info!(
                inbound_tag = %inbound.tag,
                protocol = inbound.protocol.protocol_name(),
                listen_address = %inbound.listen.address,
                listen_port = inbound.listen.port,
                reason = "config_changed",
                "signalled shutdown for changed inbound listener"
            );
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let bound = match prebound {
            Some(bound) => Ok(bound),
            None => bind_inbound_with_retry(protocols, runtime_factory, source_dir, inbound).await,
        };
        let error = match bound {
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
                    info!(
                        inbound_tag = %inbound.tag,
                        protocol = inbound.protocol.protocol_name(),
                        listen_address = %inbound.listen.address,
                        listen_port = inbound.listen.port,
                        reason = "config_reconciled",
                        "started new inbound listener"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(
                        inbound_tag = %inbound.tag,
                        protocol = inbound.protocol.protocol_name(),
                        listen_address = %inbound.listen.address,
                        listen_port = inbound.listen.port,
                        reason = "listener_prepare_error",
                        error = %error,
                        "failed to prepare inbound listener"
                    );
                    error
                }
            },
            Err(error) => {
                warn!(
                    inbound_tag = %inbound.tag,
                    protocol = inbound.protocol.protocol_name(),
                    listen_address = %inbound.listen.address,
                    listen_port = inbound.listen.port,
                    reason = "listener_bind_error",
                    error = %error,
                    "failed to bind inbound listener"
                );
                error
            }
        };
        if let Some(previous) = previous {
            let (rollback_tx, rollback_rx) = watch::channel(false);
            match bind_inbound_with_retry(
                protocols,
                rollback_runtime_factory,
                source_dir,
                &previous,
            )
            .await
            {
                Ok(bound) => match spawn_inbound_listener_with_state(
                    protocols,
                    source_dir,
                    rollback_runtime_factory,
                    &previous,
                    bound,
                    rollback_rx,
                    state.listeners,
                    true,
                ) {
                    Ok(()) => {
                        info!(
                            inbound_tag = %previous.tag,
                            protocol = previous.protocol.protocol_name(),
                            listen_address = %previous.listen.address,
                            listen_port = previous.listen.port,
                            reason = "reload_rollback",
                            "restored previous inbound listener"
                        );
                        state
                            .listener_stops
                            .insert(previous.tag.clone(), rollback_tx);
                        state.active_inbounds.insert(previous.tag.clone(), previous);
                    }
                    Err(rollback_error) => {
                        warn!(
                            inbound_tag = %inbound.tag,
                            reason = "rollback_prepare_error",
                            %rollback_error,
                            "failed to prepare previous inbound during rollback"
                        );
                    }
                },
                Err(rollback_error) => {
                    warn!(
                        inbound_tag = %inbound.tag,
                        reason = "rollback_bind_error",
                        %rollback_error,
                        "failed to rebind previous inbound during rollback"
                    );
                }
            }
        }
        return Err(error);
    }
    Ok(())
}

async fn wait_listener_stopped(shutdown: &watch::Sender<bool>) -> Result<(), EngineError> {
    tokio::time::timeout(std::time::Duration::from_secs(5), shutdown.closed())
        .await
        .map_err(|_| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "listener shutdown was not confirmed",
            ))
        })
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
