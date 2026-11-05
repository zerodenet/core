//! Explicit cleanup of an exact rejected device pass, preserving current users.
use super::*;
impl ComponentCatalog {
    /// Retain the candidate in catalog state across every join/close await.
    /// Only exclusive candidate slots are retired; shared healthy current slots
    /// retain their exact Arc, Scope and transferred business users.
    pub async fn discard_device_candidate(
        &self,
        config: &Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        self.drive_supervised(self.discard_device_inner(config, deadline))
            .await
    }
    async fn discard_device_inner(
        &self,
        config: &Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let _transition = tokio::time::timeout_at(deadline, self.transition.lock())
            .await
            .map_err(|_| {
                invalid(
                    Reason::Timeout,
                    "candidate cleanup transition remains owned",
                )
            })?;
        let (plans, keep, replacement) = {
            let state = self.state.lock().expect("component catalog poisoned");
            if state.maintenance {
                return Err(invalid(
                    Reason::StalePlan,
                    "maintenance still owns the component transition",
                ));
            }
            let mut plans: Vec<Arc<ComponentPlan>> = Vec::new();
            for entry in state
                .cold
                .iter()
                .chain(state.staged.iter())
                .filter(|entry| entry.matches(config))
            {
                if let Ok(plan) = &entry.plan {
                    if !plans.iter().any(|known| Arc::ptr_eq(known, plan)) {
                        plans.push(plan.clone());
                    }
                }
            }
            let replacement = state
                .replacement
                .as_ref()
                .filter(|pending| {
                    plans
                        .iter()
                        .any(|plan| Arc::ptr_eq(plan, &pending.candidate))
                })
                .cloned();
            if state
                .replacement
                .as_ref()
                .is_some_and(|pending| !pending.released() && replacement.is_none())
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "another original replacement remains unconfirmed",
                ));
            }
            (
                plans,
                state.current.as_ref().map(|entry| entry.ready().clone()),
                replacement,
            )
        };
        if let Some(pending) = &replacement {
            Self::finish_replacement(pending, deadline, |terminal| {
                terminal
                    .failure()
                    .map_or(Ok(()), |failure| Err(failure.retained()))
            })
            .await?;
        }
        for plan in &plans {
            plan.shutdown_except(deadline, keep.as_deref()).await?;
        }
        let mut state = self.state.lock().expect("component catalog poisoned");
        if state
            .cold
            .as_ref()
            .is_some_and(|entry| entry.matches(config))
        {
            state.cold = None;
        }
        if state
            .staged
            .as_ref()
            .is_some_and(|entry| entry.matches(config))
        {
            state.staged = None;
        }
        if replacement.as_ref().is_some_and(|original| {
            state
                .replacement
                .as_ref()
                .is_some_and(|pending| Arc::ptr_eq(pending, original))
        }) {
            state.replacement = None;
        }
        state.prune_issued();
        Ok(())
    }
}
