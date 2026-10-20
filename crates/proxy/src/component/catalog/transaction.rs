//! Persisted staging and explicit confirmation; cold readers cannot publish.
use super::*;

impl ComponentCatalog {
    pub(crate) fn stop_business(&self) {
        self.admission.stop_business();
    }
    /// Retain the actual candidate before its first start suspension. Without
    /// an actual typed supervisor handoff, task faults remain sticky here.
    pub(crate) async fn start(
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
        let _transition = tokio::time::timeout_at(deadline, self.transition.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, "component transition remains owned"))?;
        self.finish_maintenance(deadline).await?;
        let pending = self
            .state
            .lock()
            .expect("component catalog poisoned")
            .replacement
            .clone();
        if let Some(pending) = pending {
            Self::finish_replacement(&pending, deadline, &mut publish).await?;
        }
        let plan = self
            .prepare(config.clone())
            .map_err(crate::component::instance::retained)?;
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
                    // This records current's affected set only. It is not a
                    // StopOld/user closure proof or permission for shared start.
                    state.replacement = Some(pending.clone());
                    pending
                }
            }
        };
        Self::finish_replacement(&replacement, deadline, &mut publish).await?;
        {
            let mut state = self.state.lock().expect("component catalog poisoned");
            state.retired = None;
            state.cold = None;
            state.staged = Some(Current {
                config,
                plan: Ok(plan.clone()),
            });
            state.prune_issued();
            self.admission
                .stage(plan.order.iter().map(|slot| slot.admission_grant()));
        }
        plan.start(deadline).await
    }

    async fn finish_replacement(
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
    pub(crate) fn commit<T>(
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
            if !Arc::ptr_eq(previous.ready(), &plan) {
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
        state.replacement = None;
        state.prune_issued();
        Ok(result)
    }
    pub(crate) async fn shutdown(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
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
        *self.state.lock().expect("component catalog poisoned") = State::default();
        Ok(())
    }
}
