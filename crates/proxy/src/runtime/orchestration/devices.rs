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
        let prepared = self.prepare_network_devices(proxy, &snapshot).await;
        // A superseded candidate remains owned until this exact graph and
        // its real native originals have completed their separate cleanup.
        if proxy.egress_interface.generation() != generation {
            drop(prepared);
            let cleanup = proxy
                .protocols
                .discard_device_components(
                    snapshot.config(),
                    Instant::now() + Duration::from_secs(5),
                )
                .await;
            if let Err(error) = cleanup {
                self.device_retry_at = Some(Instant::now() + self.device_retry_delay);
                self.publish_network_recovery(
                    proxy,
                    generation,
                    zero_api::EndpointRecoveryPhase::Retrying,
                    Some(self.device_retry_delay.as_millis() as u64),
                    Some(error.to_string()),
                );
            } else {
                self.publish_network_recovery(
                    proxy,
                    generation,
                    zero_api::EndpointRecoveryPhase::Superseded,
                    None,
                    None,
                );
            }
            return;
        }
        match prepared {
            Ok(prepared) => {
                let publication = async {
                    proxy
                        .protocols
                        .scan_components(snapshot.config(), Instant::now() + Duration::from_secs(5))
                        .await?;
                    self.preparing_inbounds.ensure_open()?;
                    if proxy.egress_interface.generation() != generation {
                        return Err(zero_engine::EngineError::Io(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "network candidate was superseded before publication",
                        )));
                    }
                    proxy
                        .protocols
                        .commit_components(snapshot.config(), || Ok(()))
                        .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))
                }
                .await;
                if let Err(error) = publication {
                    drop(prepared);
                    if let Err(cleanup) = proxy
                        .protocols
                        .discard_device_components(
                            snapshot.config(),
                            Instant::now() + Duration::from_secs(5),
                        )
                        .await
                    {
                        warn!(%cleanup, "network publication cleanup remains owned");
                    }
                    self.device_retry_at = Some(Instant::now() + self.device_retry_delay);
                    self.publish_network_recovery(
                        proxy,
                        generation,
                        zero_api::EndpointRecoveryPhase::Retrying,
                        Some(self.device_retry_delay.as_millis() as u64),
                        Some(error.to_string()),
                    );
                    return;
                }
                prepared.publish().await;
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
                if let Err(cleanup) = proxy
                    .protocols
                    .discard_device_components(
                        snapshot.config(),
                        Instant::now() + Duration::from_secs(5),
                    )
                    .await
                {
                    warn!(%cleanup, "network candidate original cleanup remains owned");
                }
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
    async fn prepare_network_devices(
        &self,
        proxy: &Proxy,
        snapshot: &std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> Result<crate::adapters::devices::PreparedDevices, zero_engine::EngineError> {
        let config = snapshot.config();
        proxy
            .protocols
            .discard_device_components(config, Instant::now() + Duration::from_secs(5))
            .await?;
        proxy
            .protocols
            .prepare_device_components(config.clone())
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))?;
        proxy
            .protocols
            .start_components(config.clone(), Instant::now() + Duration::from_secs(5))
            .await?;
        let services = proxy.tcp_runtime_services_for_snapshot(snapshot.clone());
        proxy
            .protocols
            .start_device_resources(
                config,
                &services,
                Instant::now() + Duration::from_secs(5),
                self.preparing_inbounds.cancellation(),
            )
            .await?;
        proxy.protocols.prepare_devices(config, services).await
    }
}
