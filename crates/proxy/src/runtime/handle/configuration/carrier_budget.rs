//! Recover the existing paused application after its concrete carrier barrier.
use super::*;
impl ProxyHandle {
    pub(super) async fn restore_carrier_budget_application(
        &self,
        previous: Arc<zero_engine::EngineRuntimeSnapshot>,
        candidate: Arc<RuntimeConfig>,
        timeout: Duration,
        proxy_attempted: bool,
        mut error: String,
    ) -> String {
        let Some(reconciler) = &self.config_reconciler else {
            return error;
        };
        // No old-config native provider may be reconstructed until the exact
        // prepared application resources have really retired.
        if let Err(cleanup) = reconciler.retire_candidate(candidate).await {
            error.push_str(&format!(
                "; application carrier cleanup remains unconfirmed: {cleanup}"
            ));
            return error;
        }
        if proxy_attempted {
            if let Err(source) = self.proxy.engine.configuration_source().rollback_pending() {
                error.push_str(&format!("; source recovery remains unconfirmed: {source}"));
                return error;
            }
            // Use the original serialized proxy recovery transaction and its
            // actual native/Scope barriers, even after an earlier failed
            // attempt restored some of its old components. No error string
            // or snapshot equality is interpreted as permission to reconnect.
            if let Err(recovery) = self
                .rollback_proxy_dns_under_guard(previous.clone(), timeout)
                .await
            {
                error.push_str(&format!("; proxy recovery remains unconfirmed: {recovery}"));
                return error;
            }
        }
        let recovery = match reconciler.reconcile(previous.config().clone()).await {
            Ok(_) => reconciler.committed(previous.config().clone()).await,
            Err(recovery) => Err(recovery),
        };
        if let Err(recovery) = recovery {
            error.push_str(&format!(
                "; application recovery remains unconfirmed: {recovery}"
            ));
        }
        error
    }
}
