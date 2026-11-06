use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use zero_config::RuntimeConfig;

use super::ProxyHandle;
mod carrier_budget;

/// Application-owned services that must follow a full runtime configuration.
///
/// The proxy owns listener/data-plane reconciliation. The binary application
/// may attach one implementation for process-level services such as event
/// dispatchers and flow hooks.
#[async_trait]
pub trait ConfigApplyReconciler: Send + Sync {
    /// Reject changes that cannot be made safely before the proxy configuration
    /// or its source file is modified.
    fn validate(&self, current: &RuntimeConfig, candidate: &RuntimeConfig) -> Result<(), String>;

    /// Reconcile application services to exactly `target`.
    ///
    /// A failed attempt may leave application services stopped. The caller
    /// restores the previous proxy configuration and invokes this method again
    /// with the previous target before returning the failure.
    async fn reconcile(&self, target: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String>;

    /// Application event networking shares the actual Host carrier total.
    /// An owner of those originals may stop its old generation and prepare its
    /// target paused before the proxy installs a changed total. Returning Some
    /// means normal reconciliation is already prepared, never activated. The
    /// default owns no such originals and retains the usual reconcile order.
    async fn prepare_carrier_budget(
        &self,
        _current: &RuntimeConfig,
        _target: Arc<RuntimeConfig>,
    ) -> Result<Option<ConfigReconcileResult>, String> {
        Ok(None)
    }

    /// Confirm retirement of the actual failed/uncommitted application attempt
    /// before Proxy/DNS dependencies can be reconstructed. A lost caller,
    /// cancellation or task exit alone is not an original native-close receipt.
    async fn retire_candidate(&self, _target: Arc<RuntimeConfig>) -> Result<(), String> {
        Err("application candidate cleanup remains unconfirmed".to_owned())
    }

    /// Finalize only the already Ready application attempt after the actual
    /// host commit. Failure reports a committed operation and cannot roll back
    /// its published configuration revision.
    async fn committed(&self, _target: Arc<RuntimeConfig>) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigReconcileResult {
    pub components: Vec<String>,
}

impl ProxyHandle {
    pub fn with_config_apply_reconciler(
        mut self,
        reconciler: Arc<dyn ConfigApplyReconciler>,
    ) -> Self {
        self.config_reconciler = Some(reconciler);
        self
    }

    pub(super) async fn apply_config_transaction_and_wait(
        &self,
        candidate: RuntimeConfig,
        timeout: Duration,
        persist: bool,
    ) -> Result<ConfigReconcileResult, String> {
        self.apply_config_transaction_and_wait_if_current(candidate, None, timeout, persist)
            .await
            .map(|result| result.expect("unconditional config apply must produce a result"))
    }

    pub(super) async fn apply_config_transaction_and_wait_if_current(
        &self,
        candidate: RuntimeConfig,
        expected_current: Option<&RuntimeConfig>,
        timeout: Duration,
        persist: bool,
    ) -> Result<Option<ConfigReconcileResult>, String> {
        self.apply_config_transaction_with_impact(
            candidate,
            expected_current.cloned(),
            timeout,
            persist,
            vec![],
            vec![],
        )
        .await
    }

    pub(super) async fn apply_config_transaction_with_impact(
        &self,
        mut candidate: RuntimeConfig,
        expected_current: Option<RuntimeConfig>,
        timeout: Duration,
        persist: bool,
        disabled_principals: Vec<String>,
        changed_principals: Vec<String>,
    ) -> Result<Option<ConfigReconcileResult>, String> {
        // Typed callers may supply an unprojected canonical resource. The
        // acknowledgement and application services must see the same candidate
        // that Engine will publish, not its pre-materialization shape.
        candidate
            .materialize_endpoints()
            .map_err(|error| error.to_string())?;
        let handle = self.clone();
        let operation =
            self.proxy
                .host
                .apply()
                .run(self.proxy.host.apply_lock().clone(), async move {
                    let result = handle
                        .apply_config_under_owned_guard(
                            candidate,
                            expected_current.as_ref(),
                            timeout,
                            persist,
                        )
                        .await?;
                    if result.is_some() {
                        handle.enforce_principal_impact(&disabled_principals, &changed_principals);
                    }
                    Ok(result)
                });
        // This bounds only the caller's wait. ApplyOwner retains accepted work
        // and its original ACK through actual commit or confirmed compensation.
        tokio::time::timeout(timeout, operation)
            .await
            .map_err(|_| {
                "configuration wait timed out; accepted work remains owned; query current state"
                    .to_owned()
            })?
    }

    async fn apply_config_under_owned_guard(
        &self,
        candidate: RuntimeConfig,
        expected_current: Option<&RuntimeConfig>,
        timeout: Duration,
        persist: bool,
    ) -> Result<Option<ConfigReconcileResult>, String> {
        let previous = self.proxy.engine.runtime_snapshot();
        if expected_current.is_some_and(|expected| expected != previous.config().as_ref()) {
            return Ok(None);
        }
        self.proxy
            .protocols
            .validate_runtime_candidate(&candidate)
            .map_err(|error| error.to_string())?;
        self.proxy
            .engine
            .configuration_source()
            .ensure_available()
            .map_err(|error| error.to_string())?;
        if let Some(reconciler) = &self.config_reconciler {
            reconciler.validate(previous.config().as_ref(), &candidate)?;
        }

        if self.proxy.host.apply().is_closing() {
            return Err("proxy stopped before accepting configuration changes".to_owned());
        }

        let application_prepared = if let Some(reconciler) = &self.config_reconciler {
            match reconciler
                .prepare_carrier_budget(previous.config(), Arc::new(candidate.clone()))
                .await
            {
                Ok(prepared) => prepared,
                Err(error) => {
                    return Err(self
                        .restore_carrier_budget_application(
                            previous.clone(),
                            Arc::new(candidate.clone()),
                            timeout,
                            false,
                            error,
                        )
                        .await);
                }
            }
        } else {
            None
        };
        let prepared = match self
            .apply_proxy_config_under_guard(candidate.clone(), timeout, None, false)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) if application_prepared.is_some() => {
                return Err(self
                    .restore_carrier_budget_application(
                        previous.clone(),
                        Arc::new(candidate.clone()),
                        timeout,
                        true,
                        error,
                    )
                    .await);
            }
            Err(error) => return Err(error),
        };

        let Some(reconciler) = &self.config_reconciler else {
            if let Err(error) = self
                .commit_configuration(prepared.clone(), timeout, persist)
                .await
            {
                let recovery = self
                    .confirm_source_compensation(&prepared)
                    .map_err(|recovery| format!("{error}; {recovery}"))?;
                let rollback = self.rollback_proxy_dns_under_guard(recovery, timeout).await;
                let mut message = error;
                if let Err(error) = rollback {
                    message.push_str(&format!("; proxy rollback failed: {error}"));
                } else {
                    message.push_str("; restored last-known-good configuration");
                }
                return Err(message);
            }
            return Ok(Some(ConfigReconcileResult::default()));
        };
        let candidate = Arc::new(candidate);
        let application = match application_prepared {
            Some(prepared) => Ok(prepared),
            None => reconciler.reconcile(candidate.clone()).await,
        };
        match application {
            Ok(result) => {
                if let Err(error) = self
                    .commit_configuration(prepared.clone(), timeout, persist)
                    .await
                {
                    let recovery = self
                        .retire_application_for_recovery(
                            reconciler.as_ref(),
                            &prepared,
                            candidate.clone(),
                            &error,
                        )
                        .await?;
                    let proxy_rollback = self
                        .rollback_proxy_dns_under_guard(recovery.clone(), timeout)
                        .await;
                    let app_rollback = if proxy_rollback.is_ok() {
                        match reconciler.reconcile(recovery.config().clone()).await {
                            Ok(result) => reconciler
                                .committed(recovery.config().clone())
                                .await
                                .map(|_| result),
                            Err(error) => Err(error),
                        }
                    } else {
                        Err("application rollback skipped because proxy rollback failed".to_owned())
                    };
                    let mut message = error;
                    if let Err(error) = proxy_rollback {
                        message.push_str(&format!("; proxy rollback failed: {error}"));
                    }
                    if let Err(error) = app_rollback {
                        message.push_str(&format!("; application rollback failed: {error}"));
                    } else {
                        message.push_str("; restored last-known-good configuration");
                    }
                    return Err(message);
                }
                reconciler.committed(candidate.clone()).await.map_err(|error| format!("configuration committed; application finalization failed: {error}; query current state"))?;
                Ok(Some(result))
            }
            Err(apply_error) => {
                let recovery = self
                    .retire_application_for_recovery(
                        reconciler.as_ref(),
                        &prepared,
                        candidate.clone(),
                        &format!("application service reconciliation failed: {apply_error}"),
                    )
                    .await?;
                let proxy_rollback = self
                    .rollback_proxy_dns_under_guard(recovery.clone(), timeout)
                    .await;
                let app_rollback = if proxy_rollback.is_ok() {
                    match reconciler.reconcile(recovery.config().clone()).await {
                        Ok(result) => reconciler
                            .committed(recovery.config().clone())
                            .await
                            .map(|_| result),
                        Err(error) => Err(error),
                    }
                } else {
                    Err("application rollback skipped because proxy rollback failed".to_owned())
                };
                let mut message =
                    format!("application service reconciliation failed: {apply_error}");
                if let Err(error) = proxy_rollback {
                    message.push_str(&format!("; proxy rollback failed: {error}"));
                }
                if let Err(error) = app_rollback {
                    message.push_str(&format!("; application rollback failed: {error}"));
                } else {
                    message.push_str("; restored last-known-good configuration");
                }
                Err(message)
            }
        }
    }

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
        self.proxy.host.apply().commit(|| {
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
                        .publish_prepared_snapshot(snapshot.clone())
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
        })?;
        #[cfg(feature = "raw-ip-runtime")]
        self.proxy.protocols.finish_device_publication(&component_config).await
            .map_err(|error| format!("configuration committed; device completion remains owned: {error}; query current state"))?;
        self.proxy.publish_endpoint_facts(&snapshot);
        self.proxy.engine.configuration_source().finalize_committed()
            .map_err(|error| format!("configuration committed; source finalization failed: {error}; query current state"))
    }

    pub(super) async fn retire_application_for_recovery(
        &self,
        reconciler: &dyn ConfigApplyReconciler,
        prepared: &Arc<zero_engine::EngineRuntimeSnapshot>,
        target: Arc<RuntimeConfig>,
        original: &str,
    ) -> Result<Arc<zero_engine::EngineRuntimeSnapshot>, String> {
        if prepared.configuration_publication().is_committed() {
            let finalization = reconciler.committed(target).await;
            return Err(format!(
                "{original}; configuration was already committed; query current state{}",
                finalization
                    .err()
                    .map(|error| format!("; application finalization failed: {error}"))
                    .unwrap_or_default()
            ));
        }
        // Drive both retained obligations, but reconstruct no dependencies
        // unless every actual application and source outcome is confirmed.
        let application = reconciler.retire_candidate(target).await;
        let source = self.confirm_source_compensation(prepared);
        match (application, source) {
            (Ok(()), Ok(snapshot)) => Ok(snapshot),
            (application, source) => Err(format!("{original}; application cleanup: {}; source cleanup: {}; proxy recovery was not started",
                application.err().unwrap_or_else(|| "confirmed".to_owned()),
                source.err().unwrap_or_else(|| "confirmed".to_owned()))),
        }
    }

    pub(super) fn confirm_source_compensation(
        &self,
        candidate: &Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> Result<Arc<zero_engine::EngineRuntimeSnapshot>, String> {
        if candidate.configuration_publication().is_committed() {
            return Err("configuration was already committed; query current state; resource rollback was not attempted".to_owned());
        }
        self.proxy
            .engine
            .configuration_source()
            .rollback_pending()
            .map_err(|error| {
                format!(
                    "source recovery remains unconfirmed: {error}; resource recovery is blocked"
                )
            })?;
        Ok(self.proxy.engine.runtime_snapshot())
    }

    pub(super) async fn retire_prepared_dns(
        &self,
        timeout: Duration,
    ) -> Result<zero_dns::DnsReloadResources, String> {
        let token = self
            .proxy
            .resolver
            .prepared_reload_resources()
            .map_err(|error| format!("DNS resource candidate is unavailable: {error}"))?;
        self.proxy
            .resolver
            .retire_reload_resources(&token, tokio::time::Instant::now() + timeout)
            .await
            .map_err(|error| format!("DNS resource retirement remains unconfirmed: {error}"))?;
        Ok(token)
    }

    async fn rollback_proxy_dns_under_guard(
        &self,
        previous: Arc<zero_engine::EngineRuntimeSnapshot>,
        timeout: Duration,
    ) -> Result<(), String> {
        self.proxy.resolver.discard_prepared_reload();
        let result = self
            .apply_proxy_config_under_guard(
                (**previous.config()).clone(),
                timeout,
                Some(previous.clone()),
                true,
            )
            .await;
        result?;
        // Retirement may have closed the previous persistent backend before a
        // later commit failed. Recovery must confirm and publish the exact new
        // old-config candidate; discarding it cannot revive a closed pool.
        let token = self.retire_prepared_dns(timeout).await?;
        let component_config = previous.config().clone();
        self.proxy
            .protocols
            .scan_components(&component_config, tokio::time::Instant::now() + timeout)
            .await
            .map_err(|error| error.to_string())?;
        self.proxy.host.apply().compensate(|| {
            self.proxy
                .protocols
                .commit_components(&component_config, || {
                    self.proxy
                        .resolver
                        .commit_reload_resources(&token)
                        .map_err(|error| format!("DNS recovery commit failed: {error}"))
                })?;
            self.proxy
                .inbound_owners
                .finish_recovery_transition(&component_config);
            Ok(())
        })?;
        #[cfg(feature = "raw-ip-runtime")]
        self.proxy
            .protocols
            .finish_device_publication(&component_config)
            .await
            .map_err(|error| error.to_string())?;
        self.proxy.publish_endpoint_facts(&previous);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
