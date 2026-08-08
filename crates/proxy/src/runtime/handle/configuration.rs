use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use zero_config::RuntimeConfig;

use super::ProxyHandle;

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
        let _apply_guard = self.proxy.reload_apply_lock.lock().await;
        let previous = self.proxy.engine.config();
        if expected_current.is_some_and(|expected| expected != previous.as_ref()) {
            return Ok(None);
        }
        if let Some(reconciler) = &self.config_reconciler {
            reconciler.validate(previous.as_ref(), &candidate)?;
        }

        self.apply_proxy_config_under_guard(candidate.clone(), timeout, persist)
            .await?;

        let Some(reconciler) = &self.config_reconciler else {
            self.proxy.engine.commit_config_change();
            return Ok(Some(ConfigReconcileResult::default()));
        };
        let candidate = Arc::new(candidate);
        match reconciler.reconcile(candidate).await {
            Ok(result) => {
                self.proxy.engine.commit_config_change();
                Ok(Some(result))
            }
            Err(apply_error) => {
                let proxy_rollback = self
                    .apply_proxy_config_under_guard((*previous).clone(), timeout, persist)
                    .await;
                let app_rollback = if proxy_rollback.is_ok() {
                    reconciler.reconcile(previous).await
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
