//! Actual application/proxy prepare, compensation and commit ordering.
use super::*;
impl ProxyHandle {
    pub(super) async fn apply_config_after_control_prepared(
        &self,
        candidate: RuntimeConfig,
        previous: Arc<zero_engine::EngineRuntimeSnapshot>,
        timeout: Duration,
        persist: bool,
    ) -> Result<Option<ConfigReconcileResult>, String> {
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
}
