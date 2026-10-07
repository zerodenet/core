//! Persistent outbound device recovery after a physical-egress change.

use std::time::Duration;

use tokio::time::Instant;
use tracing::{info, warn};

use super::state::OrchestrationState;
use crate::runtime::Proxy;

impl OrchestrationState {
    fn publish_network_recovery(
        &self,
        proxy: &Proxy,
        generation: u64,
        phase: zero_api::EndpointRecoveryPhase,
        retry_after_ms: Option<u64>,
        error: Option<String>,
    ) {
        for binding in self.applied_snapshot.config().endpoint_bindings() {
            if binding.outbound_tags.is_empty() {
                continue;
            }
            let query = zero_api::EndpointGetQuery {
                endpoint_id: binding.endpoint_id.clone(),
            };
            if proxy
                .engine
                .endpoint_snapshot_in(&self.applied_snapshot, &query)
                .is_ok_and(|e| e.enabled && (e.allowed.inbound || e.allowed.outbound))
            {
                proxy.engine.record_endpoint_recovery(
                    &binding.endpoint_id,
                    zero_api::EndpointRecovery {
                        network_generation: generation,
                        phase,
                        retry_after_ms,
                        error: error.clone(),
                        observed_at_unix_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    },
                );
            }
        }
    }

    pub(super) async fn reconcile_outbound_network(&mut self, proxy: &Proxy) {
        let generation = *self.egress_updates.borrow_and_update();
        if generation == self.device_generation {
            self.device_retry_at = None;
            return;
        }
        let snapshot = self.applied_snapshot.clone();
        self.publish_network_recovery(
            proxy,
            generation,
            zero_api::EndpointRecoveryPhase::Preparing,
            None,
            None,
        );
        let prepared = proxy
            .protocols
            .prepare_outbound_devices(
                snapshot.config(),
                proxy.tcp_runtime_services_for_snapshot(snapshot.clone()),
            )
            .await;
        // Preparation can await endpoint resolution. A candidate bound to an
        // obsolete topology must never replace the committed devices.
        if proxy.egress_interface.generation() != generation {
            self.publish_network_recovery(
                proxy,
                generation,
                zero_api::EndpointRecoveryPhase::Superseded,
                None,
                None,
            );
            return;
        }
        match prepared {
            Ok(prepared) => {
                for device in prepared {
                    device.publish().await;
                }
                self.publish_endpoint_facts(proxy);
                self.publish_network_recovery(
                    proxy,
                    generation,
                    zero_api::EndpointRecoveryPhase::Recovered,
                    None,
                    None,
                );
                self.device_generation = generation;
                self.device_retry_at = None;
                self.device_retry_delay = Duration::from_secs(1);
                info!(
                    generation,
                    reason = "egress_changed",
                    "outbound devices reconciled after network change"
                );
            }
            Err(error) => {
                self.device_retry_at = Some(Instant::now() + self.device_retry_delay);
                self.publish_network_recovery(
                    proxy,
                    generation,
                    zero_api::EndpointRecoveryPhase::Retrying,
                    Some(self.device_retry_delay.as_millis() as u64),
                    Some(error.to_string()),
                );
                warn!(
                    generation,
                    retry_ms = self.device_retry_delay.as_millis() as u64,
                    %error,
                    "outbound device network reconciliation failed; retrying"
                );
                self.device_retry_delay =
                    (self.device_retry_delay * 2).min(Duration::from_secs(30));
            }
        }
    }
}
