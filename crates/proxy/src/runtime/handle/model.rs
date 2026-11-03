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
        restore: Option<std::sync::Arc<zero_engine::EngineRuntimeSnapshot>>,
        compensating: bool,
    ) -> Result<std::sync::Arc<zero_engine::EngineRuntimeSnapshot>, String> {
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

        let (receiver, candidate) =
            self.begin_acknowledged_reload(config, restore, compensating)?;
        // The accepted host operation owns this exact acknowledgement. A
        // waiting caller's deadline/cancellation cannot clear it, construct a
        // second recovery candidate, or certify original resource cleanup.
        receiver.await.map_err(|_| {
            "proxy reload acknowledgement was lost; resource cleanup remains unconfirmed; query current state".to_owned()
        })??;
        Ok(candidate)
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
        self.apply_config_transaction_with_impact(
            config,
            None,
            timeout,
            false,
            disabled_principals,
            changed_principals,
        )
        .await?;
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
            .apply_config_transaction_with_impact(
                config,
                Some(expected_current.clone()),
                timeout,
                false,
                disabled_principals,
                changed_principals,
            )
            .await?;
        if result.is_none() {
            return Ok(false);
        }
        Ok(true)
    }

    pub(super) fn enforce_principal_impact(
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
        restore: Option<std::sync::Arc<zero_engine::EngineRuntimeSnapshot>>,
        compensating: bool,
    ) -> Result<
        (
            tokio::sync::oneshot::Receiver<Result<(), String>>,
            std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
        ),
        String,
    > {
        let candidate = match restore {
            Some(snapshot) => snapshot,
            None => self
                .proxy
                .engine
                .prepare_configuration_snapshot(
                    zero_runtime::configuration::PreparedConfiguration::prepare(config)
                        .map_err(|error| error.to_string())?,
                )
                .map_err(|error| error.to_string())?,
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut pending = self
            .proxy
            .host
            .reload_ack()
            .lock()
            .expect("reload ack lock poisoned");
        {
            if pending.is_some() {
                return Err("another acknowledged proxy reload is already pending".to_owned());
            }
            *pending = Some(super::super::PendingReloadAck {
                expected: candidate.clone(),
                previous: self.proxy.engine.runtime_snapshot(),
                compensating,
                sender,
            });
        }
        drop(pending);
        self.proxy.engine.notify_configuration_reconcile();
        Ok((receiver, candidate))
    }
}
