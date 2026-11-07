//! Final same-owner DNS, control-budget and Engine publication.
use super::*;
impl ProxyHandle {
    pub(in crate::runtime) async fn commit_configuration(
        &self,
        snapshot: Arc<zero_engine::EngineRuntimeSnapshot>,
        timeout: Duration,
        persist: bool,
    ) -> Result<(), String> {
        if self.proxy.host.apply().is_closing() {
            return Err("proxy stopped before configuration commit".to_owned());
        }
        let token = self.retire_prepared_dns(timeout).await?;
        let revoked_directions =
            snapshot.revoked_endpoint_directions(&self.proxy.engine.runtime_snapshot());
        let component_config = snapshot.config().clone();
        self.proxy
            .protocols
            .scan_components(&component_config, tokio::time::Instant::now() + timeout)
            .await
            .map_err(|error| error.to_string())?;
        let control = self
            .proxy
            .host
            .control_budget()
            .candidate_for_publication(
                zero_runtime::configuration::ControlBudgetLimits::from_runtime(
                    &component_config.runtime,
                ),
            )
            .map_err(|error| error.to_string())?;
        let committed = self.proxy.host.apply().commit(|| {
            if persist {
                self.proxy
                    .engine
                    .configuration_source()
                    .persist_ready(
                        &component_config,
                        &format!(
                            "{}:{}",
                            self.proxy.engine.core_instance_id(),
                            self.proxy.engine.config_revision()
                        ),
                    )
                    .map_err(|error| format!("configuration persistence failed: {error}"))?;
            }
            self.proxy
                .protocols
                .commit_components(&component_config, || {
                    self.proxy
                        .resolver
                        .commit_reload_resources(&token)
                        .map_err(|error| format!("DNS config commit failed: {error}"))?;
                    self.proxy
                        .engine
                        .publish_prepared_snapshot_with_control(snapshot.clone(), control.as_ref())
                        .map_err(|error| error.to_string())?;
                    self.proxy.engine.commit_config_change();
                    Ok(())
                })?;
            self.proxy
                .inbound_owners
                .finish_recovery_transition(&component_config);
            for (binding, directions) in revoked_directions {
                self.proxy
                    .engine
                    .cancel_endpoint_flows(&binding, directions);
            }
            Ok(())
        });
        // Even a later enclosing finalization error cannot discard a positively
        // published metadata obligation. Wakes/restart occur outside both the
        // actual catalog/device guards and the Host's short commit gate.
        if let Some(control) = &control {
            if control
                .metadata_committed()
                .map_err(|error| error.to_string())?
            {
                control
                    .publish_committed()
                    .map_err(|error| error.to_string())?;
            }
        }
        committed?;
        #[cfg(feature = "raw-ip-runtime")]
        self.proxy.protocols.finish_device_publication(&component_config).await
            .map_err(|error| format!("configuration committed; device completion remains owned: {error}; query current state"))?;
        #[cfg(feature = "raw-ip-runtime")]
        self.proxy.confirm_tun_packet_budget(&component_config)
            .map_err(|error| format!("configuration committed; System TUN completion remains owned: {error}; query current state"))?;
        self.proxy.publish_endpoint_facts(&snapshot);
        self.proxy.engine.configuration_source().finalize_committed()
            .map_err(|error| format!("configuration committed; source finalization failed: {error}; query current state"))
    }
}
