//! Persistent outbound device recovery after a physical-egress change.

use std::time::Duration;

use tokio::time::Instant;
use tracing::{info, warn};

use super::state::OrchestrationState;
use crate::runtime::Proxy;

impl OrchestrationState {
    pub(super) async fn reconcile_outbound_network(&mut self, proxy: &Proxy) {
        let generation = *self.egress_updates.borrow_and_update();
        if generation == self.device_generation {
            self.device_retry_at = None;
            return;
        }
        let snapshot = self.applied_snapshot.clone();
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
            return;
        }
        match prepared {
            Ok(prepared) => {
                for device in prepared {
                    device.publish().await;
                }
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
