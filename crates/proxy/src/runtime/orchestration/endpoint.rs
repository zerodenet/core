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
                endpoint_id: binding.endpoint_id,
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
            proxy
                .protocols
                .observe_endpoint(self.applied_snapshot.config(), &mut endpoint);
            proxy.engine.record_endpoint_runtime_state(&endpoint);
        }
    }

    pub(super) fn validate_endpoint_directions(
        &self,
        proxy: &Proxy,
        candidate: &EngineRuntimeSnapshot,
    ) -> Result<(), EngineError> {
        for (binding, directions) in candidate.revoked_endpoint_directions(&self.applied_snapshot) {
            let query = EndpointGetQuery {
                endpoint_id: binding.endpoint_id,
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
            if previous.state == EndpointRuntimeState::Running && directions.outbound {
                return Err(EngineError::InvalidPlan { message:format!(
                    "endpoint `{}` live outbound direction contraction requires disabling it before changing directions",
                    previous.endpoint_id,
                ) });
            }
        }
        Ok(())
    }
}
