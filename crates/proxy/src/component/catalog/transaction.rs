//! Persisted staging and explicit confirmation; cold readers cannot publish.
use super::*;

impl ComponentCatalog {
    pub(crate) fn stop_business(&self) {
        self.admission.stop_business();
    }
    /// Retain the actual candidate before its first start suspension. Historical
    /// cold preparation cannot replace this owner or grant it admission.
    pub(crate) async fn start(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let _transition = tokio::time::timeout_at(deadline, self.transition.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, "component transition remains owned"))?;
        self.finish_maintenance(deadline).await?;
        let plan = self
            .prepare(config.clone())
            .map_err(crate::component::instance::retained)?;
        let (staged, retired, current) = {
            let state = self.state.lock().expect("component catalog poisoned");
            (
                state.staged.as_ref().map(|entry| entry.ready().clone()),
                state.retired.clone(),
                state.current.as_ref().map(|entry| entry.ready().clone()),
            )
        };
        for old in staged.iter().chain(retired.iter()) {
            if !Arc::ptr_eq(old, &plan) {
                old.shutdown_except(deadline, current.as_deref()).await?;
            }
        }
        {
            let mut state = self.state.lock().expect("component catalog poisoned");
            state.retired = None;
            state.cold = None;
            state.staged = Some(Current {
                config,
                plan: Ok(plan.clone()),
            });
            self.admission
                .stage(plan.order.iter().map(|slot| slot.admission_grant()));
        }
        plan.start(deadline).await
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
