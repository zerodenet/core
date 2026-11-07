//! One concrete control candidate in the existing accepted Apply operation.
use super::*;
use zero_runtime::configuration::{ControlBudgetCandidate, ControlBudgetLimits};
impl ProxyHandle {
    pub(super) async fn prepare_control_transition(
        &self,
        target: &RuntimeConfig,
        timeout: Duration,
    ) -> Result<Option<ControlBudgetCandidate>, String> {
        let book = self.proxy.host.control_budget();
        // Only an already accepted, serialized operation reaches this point.
        // The original book retains a dropped predecessor's actual token.
        if let Some(previous) = book.retained_candidate() {
            self.retire_original_control_transition(&previous, timeout)
                .await?;
        }
        let limits = ControlBudgetLimits::from_runtime(&target.runtime);
        if book.limits() == limits {
            return Ok(None);
        }
        let candidate = book.prepare(limits).map_err(|error| error.to_string())?;
        if let Err(original) = candidate
            .retire_command_scope(tokio::time::Instant::now() + timeout)
            .await
        {
            // The observer keeps its original timeout. This same accepted
            // operation continues cleanup, without starting another Scope or
            // releasing the old permission just because the waiter returned.
            let cleanup = candidate.finish_original_command_retirement().await;
            let settlement = candidate.retire();
            let mut message = original.to_string();
            if let Err(error) = cleanup {
                message.push_str(&format!("; original command cleanup: {error}"));
            }
            if let Err(error) = settlement {
                message.push_str(&format!("; retained control candidate: {error}"));
            }
            return Err(message);
        }
        Ok(Some(candidate))
    }
    async fn retire_original_control_transition(
        &self,
        candidate: &ControlBudgetCandidate,
        timeout: Duration,
    ) -> Result<(), String> {
        let initial = candidate
            .retire_command_scope(tokio::time::Instant::now() + timeout)
            .await;
        let continued = if initial.is_err() {
            candidate.finish_original_command_retirement().await
        } else {
            Ok(())
        };
        let settlement = candidate.retire();
        if let Err(error) = settlement {
            return Err(format!(
                "original control candidate remains retained: {error}"
            ));
        }
        if let Err(error) = initial {
            return Err(format!("original control candidate retirement: {error}"));
        }
        continued.map_err(|error| error.to_string())
    }
    pub(super) fn finish_control_transition(
        &self,
        candidate: Option<ControlBudgetCandidate>,
        result: Result<Option<ConfigReconcileResult>, String>,
    ) -> Result<Option<ConfigReconcileResult>, String> {
        let Some(candidate) = candidate else {
            return result;
        };
        if !candidate.is_pending().map_err(|error| error.to_string())? {
            return result;
        }
        // A failed/unpublished attempt restores its one metadata transaction;
        // all application/proxy original cleanup remains with their real owners.
        candidate
            .retire()
            .map_err(|error| format!("original control candidate remains retained: {error}"))?;
        match result {
            Err(error) => Err(error),
            Ok(_) => {
                Err("configuration returned before its actual control candidate publication".into())
            }
        }
    }
}
