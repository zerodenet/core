//! Cold values, live reuse, then one batch-wide actual StopOld barrier.
use super::*;
use crate::protocol_registry::PreparedInboundBind;
use std::collections::BTreeSet;

pub(super) struct Candidate {
    pub(super) inbound: InboundConfig,
    pub(super) previous: Option<InboundConfig>,
    pub(super) prepared: Option<PreparedInboundBind>,
    reuse: bool,
    update: bool,
}
pub(super) struct Plan {
    pub(super) candidates: Vec<Candidate>,
    removed: BTreeSet<String>,
    previous: std::sync::Arc<RuntimeConfig>,
}

pub(super) fn prepare(
    protocols: &ProtocolInventory,
    runtime_factory: &InboundListenerRuntimeFactory,
    rollback_runtime_factory: &InboundListenerRuntimeFactory,
    source_dir: Option<&Path>,
    config: &RuntimeConfig,
    active: &HashMap<String, InboundConfig>,
) -> Result<Plan, EngineError> {
    // The actual last-good snapshot survives cancellation after active entries
    // were removed. Do not reconstruct recovery history from that partial map.
    let previous_config = rollback_runtime_factory.config_handle();
    let enabled: BTreeSet<_> = config
        .inbounds
        .iter()
        .filter(|inbound| {
            runtime_factory
                .endpoint_admission()
                .listener_enabled(&inbound.tag)
        })
        .map(|inbound| inbound.tag.as_str())
        .collect();
    let removed = previous_config
        .inbounds
        .iter()
        .map(|inbound| &inbound.tag)
        .chain(active.keys())
        .filter(|tag| !enabled.contains(tag.as_str()))
        .cloned()
        .collect();
    let mut candidates = Vec::new();
    for inbound in &config.inbounds {
        if !enabled.contains(inbound.tag.as_str()) {
            continue;
        }
        let current = active.get(&inbound.tag);
        let running = runtime_factory
            .owners()
            .owner(&inbound.tag)
            .is_some_and(|owner| owner.is_running());
        let mode_changed = current.is_some()
            && protocols
                .inbound_listener_requires_restart(&runtime_factory.listener_config(inbound))?;
        let reuse = running
            && !mode_changed
            && current.is_some_and(|current| {
                !requires_listener_restart(
                    &rollback_runtime_factory.listener_config(current),
                    &runtime_factory.listener_config(inbound),
                )
            });
        let update = running
            && current.is_some_and(|current| {
                current.listen == inbound.listen
                    && current.protocol.protocol_name() == inbound.protocol.protocol_name()
            });
        // A prepared operation owns only cold values. It must not reserve a
        // listener owner, create/bind a socket, or install staged profile users.
        let prepared = if reuse {
            None
        } else {
            protocols.prepare_inbound_bind(runtime_factory.listener_config(inbound), source_dir)?
        };
        candidates.push(Candidate {
            inbound: inbound.clone(),
            previous: previous_config
                .inbounds
                .iter()
                .find(|old| old.tag == inbound.tag)
                .cloned(),
            prepared,
            reuse,
            update,
        });
    }
    Ok(Plan {
        candidates,
        removed,
        previous: previous_config,
    })
}
impl Plan {
    pub(super) async fn reuse_live(
        &mut self,
        protocols: &ProtocolInventory,
        runtime_factory: &InboundListenerRuntimeFactory,
        source_dir: Option<&Path>,
        state: &mut InboundReconcileState<'_>,
    ) -> Result<(), EngineError> {
        for candidate in std::mem::take(&mut self.candidates) {
            let inbound = &candidate.inbound;
            // This is the existing adapter mutation point, not classification.
            // In particular WireGuard keeps its same live device/security state.
            let updated = !candidate.reuse
                && candidate.update
                && protocols.update_inbound_listener(
                    runtime_factory.listener_config(inbound),
                    source_dir,
                )?;
            if candidate.reuse || updated {
                runtime_factory
                    .owners()
                    .owner(&inbound.tag)
                    .expect("checked actual running inbound owner")
                    .wait_ready()
                    .await?;
                state
                    .active_inbounds
                    .insert(inbound.tag.clone(), inbound.clone());
                if updated {
                    info!(inbound_tag = %inbound.tag, reason = "listener_updated_in_place",
                        "updated inbound listener on its existing socket");
                }
            } else {
                self.candidates.push(candidate);
            }
        }
        Ok(())
    }

    pub(super) async fn stop_old(
        &mut self,
        runtime_factory: &InboundListenerRuntimeFactory,
        state: &mut InboundReconcileState<'_>,
    ) -> Result<(), EngineError> {
        let mut affected = self.removed.clone();
        affected.extend(
            self.candidates
                .iter()
                .map(|candidate| candidate.inbound.tag.clone()),
        );
        for tag in &affected {
            if let Some(previous) = self
                .previous
                .inbounds
                .iter()
                .find(|inbound| inbound.tag == *tag)
            {
                runtime_factory.owners().retain_recovery(tag, previous)?;
            }
        }
        // Request every affected stop before awaiting any individual shutdown.
        for tag in &affected {
            if let Some(shutdown) = state.listener_stops.get(tag) {
                shutdown.send_replace(true);
            }
            if let Some(owner) = runtime_factory.owners().owner(tag) {
                owner.task_access().request_stop();
            }
        }
        for tag in affected {
            if runtime_factory.owners().owner(&tag).is_some() {
                wait_listener_stopped(runtime_factory, &tag, state).await?;
                info!(%tag, reason = "replacement_stop_old",
                    "confirmed affected old inbound owner shutdown before candidate bind");
            }
        }
        Ok(())
    }
}
