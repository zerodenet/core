use std::collections::HashMap;
use std::path::Path;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{info, warn};
mod apply;
mod plan;
mod spawn;
mod stop;
pub(in crate::runtime) use spawn::spawn_inbound_listener;
use spawn::spawn_inbound_listener_with_state;
use stop::wait_listener_stopped;
use zero_config::{InboundConfig, RuntimeConfig};
use zero_engine::EngineError;

use super::bind::PendingInbound;
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
    shutdown: watch::Sender<bool>,
) -> Result<PendingInbound, EngineError> {
    super::bind::bind_listener(
        protocols,
        runtime_factory,
        source_dir,
        inbound,
        shutdown,
        std::num::NonZeroUsize::new(1).unwrap(),
    )
    .await
}

pub(in crate::runtime) async fn reconcile_inbounds(
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    runtime_factory: &InboundListenerRuntimeFactory,
    rollback_runtime_factory: &InboundListenerRuntimeFactory,
    new_config: &RuntimeConfig,
    mut state: InboundReconcileState<'_>,
) -> Result<(), EngineError> {
    // A canceled or partially failed previous pass may have retired only its
    // local views. Confirm those same persistent native owners and report their
    // original faults before a different actual-config batch may reserve.
    runtime_factory
        .owners()
        .retire_bind_reservations_reported(
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            |tag, terminal| {
                runtime_factory.publish_inbound_terminal(tag, terminal);
                Ok(())
            },
        )
        .await?;
    let mut plan = plan::prepare(
        protocols,
        runtime_factory,
        rollback_runtime_factory,
        source_dir,
        new_config,
        state.active_inbounds,
    )?;
    // This phase may publish an adapter's existing same-endpoint hot update;
    // it is deliberately separate from pure cold candidate preparation.
    plan.reuse_live(protocols, runtime_factory, source_dir, &mut state)
        .await?;
    plan.stop_old(rollback_runtime_factory, &mut state).await?;
    for candidate in plan.candidates {
        apply::apply_candidate(
            candidate,
            protocols,
            source_dir,
            runtime_factory,
            rollback_runtime_factory,
            &mut state,
        )
        .await?;
    }
    Ok(())
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

#[cfg(all(test, feature = "socks5"))]
mod tests;
