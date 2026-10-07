//! Admission changes must use the same executable lifecycle boundary whether
//! requested by endpoint control or by complete configuration replacement.

use zero_api::{EndpointGetQuery, EndpointRuntimeState};
use zero_engine::{EngineError, EngineRuntimeSnapshot};

use super::state::OrchestrationState;
use crate::runtime::Proxy;

impl OrchestrationState {
    pub(super) fn publish_endpoint_facts(&self, proxy: &Proxy) {
        for binding in self.applied_snapshot.config().endpoint_bindings() {
            let query = EndpointGetQuery {
                endpoint_id: binding.endpoint_id.clone(),
            };
            let Ok(mut endpoint) = proxy
                .engine
                .endpoint_snapshot_in(&self.applied_snapshot, &query)
            else {
                continue;
            };
            // The existing fact is a prior generation, not evidence that
            // the newly reconciled listener/device is still running.
            endpoint.state = EndpointRuntimeState::Stopped;
            let observation = proxy
                .protocols
                .observe_endpoint(self.applied_snapshot.config(), &mut endpoint);
            proxy.engine.record_endpoint_device_state(
                &endpoint,
                observation.map(|o| o.incarnations).unwrap_or_default(),
            );
        }
    }

    pub(super) fn publish_starting(&self, proxy: &Proxy, snapshot: &EngineRuntimeSnapshot) {
        for binding in snapshot.config().endpoint_bindings() {
            let query = EndpointGetQuery {
                endpoint_id: binding.endpoint_id,
            };
            if let Ok(mut next) = proxy.engine.endpoint_snapshot_in(snapshot, &query) {
                if next.enabled && (next.allowed.inbound || next.allowed.outbound) {
                    next.state = EndpointRuntimeState::Starting;
                    proxy.engine.record_endpoint_runtime_state(&next);
                }
            }
        }
    }

    pub(super) fn publish_reload_transitions(
        &self,
        proxy: &Proxy,
        candidate: &EngineRuntimeSnapshot,
    ) {
        for binding in candidate.config().endpoint_bindings() {
            let query = EndpointGetQuery {
                endpoint_id: binding.endpoint_id,
            };
            let Ok(mut next) = proxy.engine.endpoint_snapshot_in(candidate, &query) else {
                continue;
            };
            let active = next.enabled && (next.allowed.inbound || next.allowed.outbound);
            let before = self.applied_snapshot.config();
            let after = candidate.config();
            let device_config_changed = next.inbound_tags.iter().any(|tag| {
                before
                    .inbounds
                    .iter()
                    .find(|i| &i.tag == tag)
                    .map(|i| (&i.listen, &i.protocol))
                    != after
                        .inbounds
                        .iter()
                        .find(|i| &i.tag == tag)
                        .map(|i| (&i.listen, &i.protocol))
            }) || next.outbound_tags.iter().any(|tag| {
                before
                    .outbounds
                    .iter()
                    .find(|o| &o.tag == tag)
                    .map(|o| &o.protocol)
                    != after
                        .outbounds
                        .iter()
                        .find(|o| &o.tag == tag)
                        .map(|o| &o.protocol)
            });
            next.state = match (active, next.state) {
                (true, EndpointRuntimeState::Stopped | EndpointRuntimeState::Failed) => {
                    EndpointRuntimeState::Starting
                }
                (false, EndpointRuntimeState::Running | EndpointRuntimeState::Starting) => {
                    EndpointRuntimeState::Stopping
                }
                (true, EndpointRuntimeState::Running) if device_config_changed => {
                    EndpointRuntimeState::Starting
                }
                _ => continue,
            };
            proxy.engine.record_endpoint_runtime_state(&next);
        }
        for mut endpoint in self.removed_endpoints(proxy, candidate) {
            if matches!(
                endpoint.state,
                EndpointRuntimeState::Running | EndpointRuntimeState::Starting
            ) {
                endpoint.state = EndpointRuntimeState::Stopping;
                proxy.engine.record_endpoint_runtime_state(&endpoint);
            }
        }
    }

    pub(super) fn publish_removed_endpoints(
        &self,
        proxy: &Proxy,
        candidate: &EngineRuntimeSnapshot,
    ) {
        for mut endpoint in self.removed_endpoints(proxy, candidate) {
            endpoint.state = EndpointRuntimeState::Stopped;
            proxy.engine.record_endpoint_runtime_state(&endpoint);
            proxy
                .engine
                .forget_endpoint_runtime_state(&endpoint.endpoint_id);
        }
    }

    pub(super) fn publish_rejected_endpoints(
        &self,
        proxy: &Proxy,
        rejected: &EngineRuntimeSnapshot,
        message: &str,
    ) {
        let previous = self.applied_snapshot.config();
        let candidate = rejected.config();
        for binding in previous.endpoint_bindings() {
            let changed = previous
                .endpoints
                .iter()
                .find(|endpoint| endpoint.tag == binding.tag)
                != candidate
                    .endpoints
                    .iter()
                    .find(|endpoint| endpoint.tag == binding.tag)
                || binding.inbound_tags.iter().any(|tag| {
                    previous.inbounds.iter().find(|inbound| &inbound.tag == tag)
                        != candidate
                            .inbounds
                            .iter()
                            .find(|inbound| &inbound.tag == tag)
                })
                || binding.outbound_tags.iter().any(|tag| {
                    previous
                        .outbounds
                        .iter()
                        .find(|outbound| &outbound.tag == tag)
                        != candidate
                            .outbounds
                            .iter()
                            .find(|outbound| &outbound.tag == tag)
                });
            if changed {
                let query = EndpointGetQuery {
                    endpoint_id: binding.endpoint_id.clone(),
                };
                if let Ok(endpoint) = proxy
                    .engine
                    .endpoint_snapshot_in(&self.applied_snapshot, &query)
                {
                    proxy.engine.record_endpoint_runtime_error(
                        &binding.endpoint_id,
                        message,
                        endpoint.enabled
                            && (endpoint.allowed.inbound || endpoint.allowed.outbound)
                            && endpoint.state == EndpointRuntimeState::Stopped,
                    );
                }
            }
        }
        // Newly staged identities must not retain a starting fact after rollback.
        for binding in candidate.endpoint_bindings() {
            let query = EndpointGetQuery {
                endpoint_id: binding.endpoint_id,
            };
            if proxy
                .engine
                .endpoint_snapshot_in(&self.applied_snapshot, &query)
                .is_err()
            {
                if let Ok(mut endpoint) = proxy.engine.endpoint_snapshot_in(rejected, &query) {
                    endpoint.state = EndpointRuntimeState::Stopped;
                    proxy.engine.record_endpoint_runtime_state(&endpoint);
                    proxy.engine.record_endpoint_runtime_error(
                        &endpoint.endpoint_id,
                        message,
                        false,
                    );
                    proxy
                        .engine
                        .forget_endpoint_runtime_state(&endpoint.endpoint_id);
                }
            }
        }
    }

    fn removed_endpoints(
        &self,
        proxy: &Proxy,
        candidate: &EngineRuntimeSnapshot,
    ) -> Vec<zero_api::EndpointSnapshot> {
        let retained = candidate
            .config()
            .endpoint_bindings()
            .into_iter()
            .map(|binding| binding.endpoint_id)
            .collect::<std::collections::HashSet<_>>();
        self.applied_snapshot
            .config()
            .endpoint_bindings()
            .into_iter()
            .filter(|binding| !retained.contains(&binding.endpoint_id))
            .filter_map(|binding| {
                proxy
                    .engine
                    .endpoint_snapshot_in(
                        &self.applied_snapshot,
                        &EndpointGetQuery {
                            endpoint_id: binding.endpoint_id,
                        },
                    )
                    .ok()
            })
            .collect()
    }

    pub(super) fn validate_endpoint_directions(
        &self,
        proxy: &Proxy,
        candidate: &EngineRuntimeSnapshot,
    ) -> Result<(), EngineError> {
        for (binding, directions) in candidate.revoked_endpoint_directions(&self.applied_snapshot) {
            let query = EndpointGetQuery {
                endpoint_id: binding.endpoint_id.clone(),
            };
            let Ok(next) = proxy.engine.endpoint_snapshot_in(candidate, &query) else {
                // Removal closes the entire resource through normal reconcile.
                continue;
            };
            if !next.enabled {
                continue;
            }
            if next.inbound_tags != binding.inbound_tags
                || next.outbound_tags != binding.outbound_tags
            {
                // Removing configured roles is a topology change. The
                // existing listener/device reconcile closes those roles;
                // do not confuse it with revoking a retained role's policy.
                continue;
            }
            let mut previous = proxy
                .engine
                .endpoint_snapshot_in(&self.applied_snapshot, &query)
                .map_err(|error| EngineError::InvalidPlan {
                    message: error.message,
                })?;
            proxy
                .protocols
                .observe_endpoint(self.applied_snapshot.config(), &mut previous);
            if previous.state == EndpointRuntimeState::Running
                && directions.outbound
                && !proxy
                    .protocols
                    .endpoint_live_direction_contraction(&binding)
                    .outbound
            {
                return Err(EngineError::InvalidPlan { message:format!(
                    "endpoint `{}` live outbound direction contraction requires disabling it before changing directions",
                    previous.endpoint_id,
                ) });
            }
        }
        Ok(())
    }
}
