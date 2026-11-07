//! Persisted staging and explicit confirmation; cold readers cannot publish.
use super::*;

/// The same retained replacement after actual old closure and policy install,
/// before any new component owner is constructed. Drop releases only serial
/// coordination; the catalog continues to own the candidate and cleanup fence.
#[must_use]
pub struct PreparedComponentStart<'a> {
    catalog: &'a ComponentCatalog,
    config: Arc<RuntimeConfig>,
    plan: Arc<ComponentPlan>,
    _transition: tokio::sync::MutexGuard<'a, ()>,
}
impl PreparedComponentStart<'_> {
    pub fn plan(&self) -> &Arc<ComponentPlan> {
        &self.plan
    }
    /// Recompute the same cold candidate after its actual Source has changed
    /// network facts. The first cold recipes remain strongly owned so protocol
    /// material is inherited from that accepted pass rather than reread files.
    /// No built candidate or newly installed owner may cross this boundary.
    pub async fn refresh_device_resources(
        &mut self,
        deadline: tokio::time::Instant,
        prepare: impl FnOnce(
            &mut crate::component::ResourcePreparation<'_>,
        ) -> Result<std::collections::BTreeSet<ComponentId>, ChannelError>,
    ) -> Result<Arc<ComponentPlan>, ChannelError> {
        let pending = {
            let state = self.catalog.state.lock().expect("component catalog poisoned");
            state.replacement.as_ref().filter(|pending| {
                !state.maintenance && Arc::ptr_eq(&pending.candidate, &self.plan)
                    && pending.released()
                    && pending.matches_current(state.current.as_ref().map(Current::ready))
                    && state.staged.as_ref().is_none_or(|entry| !Arc::ptr_eq(entry.ready(), &self.plan))
            }).cloned().ok_or_else(|| invalid(Reason::StalePlan,
                "network refresh no longer owns its confirmed cold replacement"))?
        };
        for slot in &self.plan.order {
            slot.seal_unbuilt_refresh()?;
        }
        let (plan, replacement) = {
            let mut state = self.catalog.state.lock().expect("component catalog poisoned");
            if !state.replacement.as_ref().is_some_and(|actual| Arc::ptr_eq(actual, &pending)) {
                return Err(invalid(Reason::StalePlan, "network refresh replacement changed"));
            }
            let plan = Arc::new(self.catalog.prepare_restart_plan_with_devices(
                &self.config, Some((&self.config, &self.plan)), &state.issued, prepare,
            )?);
            let replacement = Arc::new(replacement::Replacement::new(
                plan.clone(), state.current.as_ref().map(Current::ready),
                pending.owned_plans().cloned(),
            )?);
            if let Some(pause) = pending.budget_pause() {
                replacement.retain_budget_pause(pause);
            }
            for slot in &plan.order {
                state.issued.entry(slot.id)
                    .and_modify(|floor| *floor = (*floor).max(slot.generation))
                    .or_insert(slot.generation);
            }
            state.cold = Some(Current { config: self.config.clone(), plan: Ok(plan.clone()) });
            state.replacement = Some(replacement.clone());
            state.prune_issued();
            (plan, replacement)
        };
        self.plan = plan.clone();
        // Persist all originals before this suspension. Already reported old
        // faults stay reported; never-built first-pass slots prove NoAcquisition
        // through their normal cleanup instead of a manufactured close token.
        ComponentCatalog::finish_replacement(&replacement, deadline, |terminal| {
            terminal.failure().map_or(Ok(()), |failure| Err(failure.retained()))
        }).await?;
        Ok(plan)
    }
    /// Start only this same confirmed candidate. Source preparation may have
    /// changed the real network generation before the caller reaches this step.
    pub async fn start(self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        self.catalog
            .drive_supervised(self.start_inner(deadline))
            .await
    }
    async fn start_inner(self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        {
            let mut state = self
                .catalog
                .state
                .lock()
                .expect("component catalog poisoned");
            if state.maintenance
                || !state.replacement.as_ref().is_some_and(|pending| {
                    Arc::ptr_eq(&pending.candidate, &self.plan)
                        && pending.released()
                        && pending.matches_current(state.current.as_ref().map(Current::ready))
                })
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "prepared start no longer owns its actual replacement",
                ));
            }
            state.retired = None;
            state.cold = None;
            state.staged = Some(Current {
                config: self.config.clone(),
                plan: Ok(self.plan.clone()),
            });
            state.prune_issued();
            self.catalog.admission.stage(
                self.plan
                    .startable_before_devices()
                    .map(|slot| slot.admission_grant()),
            );
        }
        self.plan.start(deadline).await
    }
}

impl ComponentCatalog {
    pub fn stop_business(&self) {
        self.admission.stop_business();
    }
    /// Retain the actual candidate before its first start suspension. Without
    /// an actual typed supervisor handoff, task faults remain sticky here.
    pub async fn start(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        self.start_reported(config, deadline, |terminal| {
            terminal
                .failure()
                .map_or(Ok(()), |failure| Err(failure.retained()))
        })
        .await
    }

    /// Explicit typed handoff seam. Production `start` conservatively refuses
    /// to consume a failure until its real caller supplies a supervisor owner.
    pub(super) async fn start_reported(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
        mut publish: impl FnMut(&replacement::DeclaredComponentTerminal) -> Result<(), ChannelError>,
    ) -> Result<(), ChannelError> {
        self.drive_supervised(self.start_reported_inner(config, deadline, &mut publish))
            .await
    }

    /// Confirm StopOld and install the exact target limits on the existing
    /// roots, leaving BuildNew sealed. The owning Source transaction retains
    /// this token while its own original device changes network facts.
    pub async fn prepare_resource_start<'a>(
        &'a self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<PreparedComponentStart<'a>, ChannelError> {
        self.drive_supervised(self.prepare_start_inner(
            config,
            deadline,
            |terminal: &replacement::DeclaredComponentTerminal| {
                terminal
                    .failure()
                    .map_or(Ok(()), |failure| Err(failure.retained()))
            },
        ))
        .await
    }
    async fn start_reported_inner(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
        publish: &mut impl FnMut(&replacement::DeclaredComponentTerminal) -> Result<(), ChannelError>,
    ) -> Result<(), ChannelError> {
        self.prepare_start_inner(config, deadline, publish)
            .await?
            .start_inner(deadline)
            .await
    }
    async fn prepare_start_inner<'a>(
        &'a self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
        mut publish: impl FnMut(&replacement::DeclaredComponentTerminal) -> Result<(), ChannelError>,
    ) -> Result<PreparedComponentStart<'a>, ChannelError> {
        let _transition = tokio::time::timeout_at(deadline, self.transition.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, "component transition remains owned"))?;
        self.finish_maintenance(deadline).await?;
        let previous_replacement = self
            .state
            .lock()
            .expect("component catalog poisoned")
            .replacement
            .clone();
        if let Some(pending) = &previous_replacement {
            Self::finish_replacement(pending, deadline, &mut publish).await?;
        }
        let loan_fence = self.admission.freeze_loans();
        let plan = self
            .prepare(config.clone())
            .map_err(crate::component::instance::retained)?;
        let changes_limits = {
            let state = self.state.lock().expect("component catalog poisoned");
            state.current.as_ref().is_some_and(|current| {
                crate::HostOriginalBudgets::limits_changed(&current.config.runtime, &config.runtime)
            })
        };
        let already_paused = previous_replacement.as_ref().is_some_and(|original| {
            Arc::ptr_eq(&original.candidate, &plan) && original.retains_budget_pause()
        });
        let budget_pause = if changes_limits && !already_paused {
            Some(self.try_pause_replacement_users(&plan)?)
        } else {
            None
        };
        let replacement = {
            let mut state = self.state.lock().expect("component catalog poisoned");
            match state
                .replacement
                .as_ref()
                .filter(|pending| Arc::ptr_eq(&pending.candidate, &plan))
            {
                Some(pending) => pending.clone(),
                None => {
                    let old = state
                        .staged
                        .iter()
                        .map(|entry| entry.ready().clone())
                        .chain(state.retired.iter().cloned())
                        .chain(
                            state
                                .replacement
                                .iter()
                                .map(|pending| pending.candidate.clone()),
                        );
                    let pending = Arc::new(replacement::Replacement::new(
                        plan.clone(),
                        state.current.as_ref().map(Current::ready),
                        old,
                    )?);
                    // Retain the exact old/current/candidate owners before
                    // StopOld can suspend. A lost waiter cannot free them.
                    state.replacement = Some(pending.clone());
                    pending
                }
            }
        };
        if let Some(pause) = budget_pause {
            replacement.retain_budget_pause(Arc::new(pause));
        }
        loan_fence.retain();
        self.admission.signal().notify_waiters();
        Self::finish_replacement(&replacement, deadline, &mut publish).await?;
        // Old affected originals and their real joins/ACKs have now returned.
        // Install on the same root before any candidate native job starts.
        self.original_budgets()
            .install_original_limits(&config.runtime)?;
        Ok(PreparedComponentStart {
            catalog: self,
            config,
            plan,
            _transition,
        })
    }

    pub(super) async fn finish_replacement(
        pending: &replacement::Replacement,
        deadline: tokio::time::Instant,
        publish: impl FnOnce(&replacement::DeclaredComponentTerminal) -> Result<(), ChannelError>,
    ) -> Result<(), ChannelError> {
        if pending.released() {
            return Ok(());
        }
        let receipt = pending.confirm(deadline).await?;
        match pending.release(receipt, publish)? {
            // A reported fault still rejects this attempt; only a subsequent
            // explicit restore/retry may use its confirmed declared closure.
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    /// Execute the real commit against the exact retained candidate, then publish
    /// its generations. No decoding, rebuilding or I/O happens under this lock.
    pub fn commit<T>(
        &self,
        config: &Arc<RuntimeConfig>,
        commit: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let mut state = self.state.lock().expect("component catalog poisoned");
        if state.maintenance {
            return Err("Service maintenance cannot publish Business admission".to_owned());
        }
        if state
            .staged
            .as_ref()
            .is_some_and(|entry| !entry.matches(config))
        {
            return Err(
                "another component candidate still requires confirmed retirement".to_owned(),
            );
        }
        let selected = state
            .staged
            .as_ref()
            .filter(|entry| entry.matches(config))
            .or_else(|| state.current.as_ref().filter(|entry| entry.matches(config)))
            .ok_or_else(|| "no started component plan for configuration commit".to_owned())?;
        let plan = selected.ready().clone();
        if state.replacement.as_ref().is_some_and(|pending| {
            !Arc::ptr_eq(&pending.candidate, &plan)
                || !pending.released()
                || !pending.matches_current(state.current.as_ref().map(Current::ready))
        }) {
            return Err(
                "replacement ownership or declared retirement remains unconfirmed".to_owned(),
            );
        }
        if !plan.started.load(Ordering::Acquire) {
            return Err("component plan has not confirmed start".to_owned());
        }
        if let Some(failure) = plan.order.iter().find_map(|slot| slot.task_fault()) {
            // This compatibility API returns String and cannot retain the
            // typed fault. Leave its publication pending for the real receiver.
            return Err(failure.error().to_string());
        }
        if !plan.can_reuse() {
            return Err("component plan contains a revoked or failed runtime owner".to_owned());
        }
        if state.retired.is_some()
            && state
                .current
                .as_ref()
                .is_some_and(|entry| !Arc::ptr_eq(entry.ready(), &plan))
        {
            return Err("previous component retirement remains unconfirmed".to_owned());
        }
        let result = commit()?;
        if let Some(previous) = state.current.take() {
            let needs_retirement = previous.ready().order.iter().any(|slot| {
                !plan.order.iter().any(|keep| Arc::ptr_eq(keep, slot))
                    && !previous
                        .ready()
                        .confirmed(&std::collections::BTreeSet::from([slot.instance_id()]))
            });
            if !Arc::ptr_eq(previous.ready(), &plan) && needs_retirement {
                state.retired = Some(previous.ready().clone());
            }
        }
        self.admission
            .publish(plan.order.iter().map(|slot| slot.admission_grant()));
        state.current = Some(Current {
            config: config.clone(),
            plan: Ok(plan),
        });
        state.staged = None;
        state.cold = None;
        let retired_replacement = state.replacement.take();
        state.prune_issued();
        drop(state);
        drop(retired_replacement); // actual pause/last owner drops outside catalog lock.
        Ok(result)
    }
    pub async fn shutdown(&self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        self.drive_supervised(self.shutdown_inner(deadline)).await
    }
    async fn shutdown_inner(&self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        let _transition = tokio::time::timeout_at(deadline, self.transition.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, "component transition remains owned"))?;
        self.admission.close();
        let plans = {
            let state = self.state.lock().expect("component catalog poisoned");
            state
                .staged
                .iter()
                .chain(state.current.iter())
                .map(|entry| entry.ready().clone())
                .chain(state.retired.iter().cloned())
                .chain(
                    state
                        .replacement
                        .iter()
                        .flat_map(|pending| pending.owned_plans().cloned()),
                )
                .collect::<Vec<_>>()
        };
        let mut failure = None;
        for plan in plans {
            if let Err(error) = plan.shutdown(deadline).await {
                failure.get_or_insert(error);
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        let retired = {
            let mut state = self.state.lock().expect("component catalog poisoned");
            std::mem::take(&mut *state)
        };
        drop(retired); // all actual guards/recipes outside the enclosing catalog lock.
        self.admission.finish_shutdown();
        Ok(())
    }
}
