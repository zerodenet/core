use crate::runtime::Proxy;

/// Wraps [`EngineHandle`] with TUN command interception.
///
/// TUN start/stop commands are handled by the proxy runtime,
/// not the engine. This wrapper intercepts those commands
/// before they reach `EngineHandle`.
#[derive(Clone)]
pub struct ProxyHandle {
    pub(super) inner: zero_engine::EngineHandle,
    pub(super) proxy: Proxy,
    pub(super) config_reconciler: Option<std::sync::Arc<dyn super::ConfigApplyReconciler>>,
}

impl ProxyHandle {
    pub fn new(inner: zero_engine::EngineHandle, proxy: Proxy) -> Self {
        Self {
            inner,
            proxy,
            config_reconciler: None,
        }
    }

    /// Access the underlying EngineHandle.
    pub fn engine_handle(&self) -> &zero_engine::EngineHandle {
        &self.inner
    }

    /// Apply a complete configuration and wait until the proxy runtime has
    /// reconciled its listeners. This is stronger than the engine-level
    /// `config.apply` acknowledgement, which only confirms plan installation.
    pub async fn apply_config_and_wait(
        &self,
        config: zero_config::RuntimeConfig,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        self.apply_config_transaction_and_wait(config, timeout, true)
            .await
            .map(|_| ())
    }

    /// Apply a runtime-only configuration overlay and wait until listener
    /// reconciliation completes.
    ///
    /// Unlike [`Self::apply_config_and_wait`], this never writes the engine's
    /// source configuration file. It is intended for narrow, remotely
    /// projected state whose owner is outside the local deployment artifact.
    pub async fn apply_runtime_config_and_wait(
        &self,
        config: zero_config::RuntimeConfig,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        self.apply_config_transaction_and_wait(config, timeout, false)
            .await
            .map(|_| ())
    }

    pub(super) async fn apply_proxy_config_under_guard(
        &self,
        config: zero_config::RuntimeConfig,
        timeout: std::time::Duration,
        persist: bool,
    ) -> Result<(), String> {
        let mut ready = self.proxy.orchestration_ready.subscribe();
        if !*ready.borrow() {
            tokio::time::timeout(timeout, async {
                while !*ready.borrow() {
                    ready
                        .changed()
                        .await
                        .map_err(|_| "proxy runtime stopped before becoming ready".to_owned())?;
                }
                Ok::<(), String>(())
            })
            .await
            .map_err(|_| "timed out waiting for proxy runtime startup".to_owned())??;
        }

        let previous = self.proxy.engine.config();
        let receiver = self.begin_acknowledged_reload(config, persist)?;

        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.clear_pending_reload();
                self.proxy.resolver.discard_prepared_reload();
                self.rollback_unconfirmed_reload(previous, timeout, persist)
                    .await?;
                Err(
                    "proxy reload acknowledgement channel closed; restored last-known-good config"
                        .to_owned(),
                )
            }
            Err(_) => {
                self.clear_pending_reload();
                self.proxy.resolver.discard_prepared_reload();
                self.rollback_unconfirmed_reload(previous, timeout, persist)
                    .await?;
                Err("timed out waiting for proxy listener reconciliation; restored last-known-good config"
                    .to_owned())
            }
        }
    }

    /// Apply a runtime-only configuration overlay, then enforce its neutral
    /// principal effects only after listener reconciliation succeeds.
    ///
    /// The caller owns the use-case semantics that produced the complete
    /// configuration and the two principal sets. The proxy only guarantees
    /// transactional reconciliation and post-success session invalidation.
    pub async fn apply_runtime_config_with_principal_impact_and_wait(
        &self,
        config: zero_config::RuntimeConfig,
        disabled_principals: Vec<String>,
        changed_principals: Vec<String>,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        self.apply_config_transaction_and_wait(config, timeout, false)
            .await?;
        self.enforce_principal_impact(&disabled_principals, &changed_principals);
        Ok(())
    }

    /// Apply a runtime-only configuration only if the effective configuration
    /// still equals the snapshot used to build it.
    ///
    /// The comparison runs while holding the same apply lock as local
    /// `config.apply`, so a caller-derived overlay cannot overwrite a newer
    /// effective configuration. `Ok(false)` asks the caller to read the latest
    /// configuration and construct a new candidate.
    pub async fn apply_runtime_config_if_current_with_principal_impact_and_wait(
        &self,
        expected_current: &zero_config::RuntimeConfig,
        config: zero_config::RuntimeConfig,
        disabled_principals: Vec<String>,
        changed_principals: Vec<String>,
        timeout: std::time::Duration,
    ) -> Result<bool, String> {
        let result = self
            .apply_config_transaction_and_wait_if_current(
                config,
                Some(expected_current),
                timeout,
                false,
            )
            .await?;
        if result.is_none() {
            return Ok(false);
        }
        self.enforce_principal_impact(&disabled_principals, &changed_principals);
        Ok(true)
    }

    fn enforce_principal_impact(
        &self,
        disabled_principals: &[String],
        changed_principals: &[String],
    ) {
        for principal_key in disabled_principals {
            self.proxy
                .engine
                .close_principal_flows(principal_key, "principal_disabled");
            self.proxy
                .engine
                .forget_principal_policy_state(principal_key);
        }
        for principal_key in changed_principals {
            self.proxy
                .engine
                .close_principal_flows(principal_key, "principal_policy_changed");
        }
    }

    fn begin_acknowledged_reload(
        &self,
        config: zero_config::RuntimeConfig,
        persist: bool,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), String>>, String> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        {
            let mut pending = self
                .proxy
                .reload_ack
                .lock()
                .expect("reload ack lock poisoned");
            if pending.is_some() {
                return Err("another acknowledged proxy reload is already pending".to_owned());
            }
            *pending = Some(super::super::PendingReloadAck {
                expected: config.clone(),
                persist,
                sender,
            });
        }
        let result = if persist {
            self.proxy.engine.stage_config(config)
        } else {
            self.proxy.engine.stage_runtime_config(config)
        };
        if let Err(error) = result {
            self.clear_pending_reload();
            return Err(error.to_string());
        }
        Ok(receiver)
    }

    fn clear_pending_reload(&self) {
        self.proxy
            .reload_ack
            .lock()
            .expect("reload ack lock poisoned")
            .take();
    }

    async fn rollback_unconfirmed_reload(
        &self,
        previous: std::sync::Arc<zero_config::RuntimeConfig>,
        requested_timeout: std::time::Duration,
        persist: bool,
    ) -> Result<(), String> {
        let receiver = self
            .begin_acknowledged_reload((*previous).clone(), persist)
            .map_err(|error| format!("failed to start last-known-good rollback: {error}"))?;
        let rollback_timeout = requested_timeout.max(std::time::Duration::from_secs(5));
        let result = match tokio::time::timeout(rollback_timeout, receiver).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(format!(
                "last-known-good rollback listener reconciliation failed: {error}"
            )),
            Ok(Err(_)) => Err("last-known-good rollback acknowledgement channel closed".to_owned()),
            Err(_) => {
                self.clear_pending_reload();
                Err("timed out waiting for last-known-good rollback reconciliation".to_owned())
            }
        };
        self.proxy.resolver.discard_prepared_reload();
        result
    }
}
