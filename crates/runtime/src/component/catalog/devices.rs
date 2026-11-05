//! Device recipes join the same cold pass; preparation grants no deferred start.
use super::*;
use crate::component::ResourcePreparation;
use std::collections::BTreeSet;

impl ComponentCatalog {
    /// Start the retained deferred graph only after this exact staged
    /// candidate's StopOld transaction has confirmed every affected original.
    /// Its real caller retains any resolve/install invocation across this wait.
    /// Ordinary Service startup cannot obtain this admission by reading a plan.
    pub async fn start_device_components(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        self.drive_supervised(self.start_devices_inner(config, deadline))
            .await
    }

    async fn start_devices_inner(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let _transition = tokio::time::timeout_at(deadline, self.transition.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, "device transition remains owned"))?;
        let plan = {
            let state = self.state.lock().expect("component catalog poisoned");
            let candidate = state
                .staged
                .as_ref()
                .filter(|entry| entry.matches(&config))
                .ok_or_else(|| invalid(Reason::StalePlan, "no exact staged device candidate"))?;
            let plan = candidate.ready();
            if state.maintenance
                || !plan.device_pass
                || plan
                    .startable_before_devices()
                    .any(|slot| !slot.has_started() || !slot.can_reuse())
                || !state.replacement.as_ref().is_some_and(|pending| {
                    Arc::ptr_eq(&pending.candidate, plan)
                        && pending.released()
                        && pending.matches_current(state.current.as_ref().map(Current::ready))
                })
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "device start requires its confirmed current replacement",
                ));
            }
            // This grants preparation Service access only. Business publication
            // still requires every real deferred start and the existing commit.
            self.admission
                .stage(plan.order.iter().map(|slot| slot.admission_grant()));
            plan.clone()
        };
        for slot in &plan.order {
            if plan.deferred_devices.contains(&slot.id) {
                slot.start(deadline).await?;
            }
        }
        plan.scan_tasks(deadline).await?;
        plan.started.store(true, Ordering::Release);
        Ok(())
    }

    /// One accepted candidate contributes its real recipes to the original
    /// allocator, canonical identity index, dependency order and generation floor.
    /// The callback is cold. Its returned IDs must come from that very pass.
    ///
    /// Unlike ordinary prepare, this deliberately compiles the device pass once;
    /// the actual caller must retain its invocation/owner until commit or cleanup.
    /// Later ordinary readers reuse the retained exact candidate. No method here
    /// stages admission or claims that old users/native/tasks have closed.
    pub fn prepare_with_device_resources(
        &self,
        config: Arc<RuntimeConfig>,
        prepare: impl FnOnce(
            &mut ResourcePreparation<'_>,
        ) -> Result<BTreeSet<ComponentId>, ChannelError>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        self.prepare_devices(config, false, prepare)
    }
    /// Explicit rollback of the exact published input retains its already
    /// prepared protocol crypto/material and rebuilds only consumed device recipes.
    pub fn prepare_recovered_device_resources(
        &self,
        config: Arc<RuntimeConfig>,
        prepare: impl FnOnce(
            &mut ResourcePreparation<'_>,
        ) -> Result<BTreeSet<ComponentId>, ChannelError>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        self.prepare_devices(config, true, prepare)
    }
    fn prepare_devices(
        &self,
        config: Arc<RuntimeConfig>,
        recovering: bool,
        prepare: impl FnOnce(
            &mut ResourcePreparation<'_>,
        ) -> Result<BTreeSet<ComponentId>, ChannelError>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        let mut state = self.state.lock().expect("component catalog poisoned");
        if state.maintenance
            || state
                .replacement
                .as_ref()
                .is_some_and(|pending| !pending.released())
        {
            return Err(Arc::new(invalid(
                Reason::StalePlan,
                "another actual component transition remains owned",
            )));
        }
        let previous = state
            .current
            .iter()
            .chain(state.staged.iter())
            .chain(state.cold.iter())
            .find_map(|entry| entry.plan.as_ref().ok().map(|plan| (&entry.config, plan)));
        let plan = if recovering {
            state
                .current
                .as_ref()
                .filter(|entry| entry.matches(&config))
                .ok_or_else(|| {
                    invalid(
                        Reason::StalePlan,
                        "recovery does not name the original published configuration",
                    )
                })
                .and_then(|entry| {
                    self.restore_published_with_devices(
                        &config,
                        entry.ready(),
                        &state.issued,
                        prepare,
                    )
                })
        } else {
            self.prepare_plan_with_devices(&config, previous, &state.issued, prepare)
        }
        .map(Arc::new)
        .map_err(Arc::new);
        if let Ok(plan) = &plan {
            for slot in &plan.order {
                state
                    .issued
                    .entry(slot.id)
                    .and_modify(|floor| *floor = (*floor).max(slot.generation))
                    .or_insert(slot.generation);
            }
        }
        state.cold = Some(Current {
            config,
            plan: plan.clone(),
        });
        state.prune_issued();
        plan
    }
}

#[cfg(test)]
#[path = "tests/device_union.rs"]
mod tests;
