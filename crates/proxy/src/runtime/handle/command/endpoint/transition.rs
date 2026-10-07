//! Intent validation precedes transitions; reconcile publishes final facts.

use zero_api::{EndpointGetQuery, EndpointRuntimeState};
use zero_engine::EngineRuntimeSnapshot;

use super::ProxyHandle;

impl ProxyHandle {
    pub(super) fn publish_endpoint_control_transitions(&self, candidate: &EngineRuntimeSnapshot) {
        for binding in candidate.config().endpoint_bindings() {
            let query = EndpointGetQuery {
                endpoint_id: binding.endpoint_id,
            };
            let Ok(mut next) = self.proxy.engine.endpoint_snapshot_in(candidate, &query) else {
                continue;
            };
            let Ok(old) = self.observed_endpoint(&query.endpoint_id) else {
                continue;
            };
            let state = match (next.enabled, old.state) {
                (
                    true,
                    EndpointRuntimeState::Stopped
                    | EndpointRuntimeState::Stopping
                    | EndpointRuntimeState::Failed,
                ) => EndpointRuntimeState::Starting,
                (false, EndpointRuntimeState::Running | EndpointRuntimeState::Starting) => {
                    EndpointRuntimeState::Stopping
                }
                _ => continue,
            };
            next.state = state;
            self.proxy.engine.record_endpoint_runtime_state(&next);
        }
    }
}

impl ProxyHandle {
    pub(super) fn endpoint_packet_roles_active(
        &self,
        binding: &zero_config::EndpointBindingConfig,
        directions: zero_api::EndpointDirections,
    ) -> bool {
        let scopes = binding
            .inbound_tags
            .iter()
            .filter(|_| directions.inbound)
            .map(|tag| zero_api::TrafficScope::Inbound { tag: tag.clone() })
            .chain(
                binding
                    .outbound_tags
                    .iter()
                    .filter(|_| directions.outbound)
                    .map(|tag| zero_api::TrafficScope::Outbound { tag: tag.clone() }),
            );
        scopes.into_iter().any(|scope| {
            self.proxy
                .engine
                .traffic_snapshot(&zero_api::TrafficGetQuery { scope })
                .is_ok_and(|snapshot| {
                    snapshot
                        .activity
                        .active_packet_routes
                        .is_some_and(|count| count > 0)
                })
        })
    }
}
