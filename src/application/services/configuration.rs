use super::*;

#[async_trait::async_trait]
impl ConfigApplyReconciler for ApplicationServices {
    fn validate(&self, current: &RuntimeConfig, candidate: &RuntimeConfig) -> Result<(), String> {
        // This executes before Proxy/DNS prepare. An unresolved application
        // original cannot authorize another candidate in a different component.
        let mut state = self.state.try_lock().map_err(|_| {
            "application preparation/retirement is still owned by its current operation".to_owned()
        })?;
        let hook_error = state
            .hooks
            .as_ref()
            .into_iter()
            .chain(state.pending_hooks.as_ref())
            .find_map(|hooks| hooks.confirm_original_io_closed().err());
        if let Some(error) = hook_error {
            state.cleanup_fault.get_or_insert_with(|| Arc::new(error));
        }
        if let Some(error) = &state.cleanup_fault {
            return Err(format!(
                "original application cleanup is unresolved: {error}"
            ));
        }
        if state
            .attempt
            .as_ref()
            .is_some_and(|attempt| self.engine.config().as_ref() != attempt.target.as_ref())
        {
            return Err("application candidate still requires confirmed retirement".to_owned());
        }
        drop(state);
        let limits =
            zero_runtime::configuration::ControlBudgetLimits::from_runtime(&candidate.runtime);
        if self.control_budget.limits() != limits {
            self.control_budget
                .validate(limits)
                .map_err(|error| error.to_string())?;
        }
        if current.api.control != candidate.api.control {
            return Err(
                "`api.control` cannot be changed by live config.apply; keep the active control endpoint and restart explicitly to replace its listener or credentials"
                    .to_owned(),
            );
        }

        #[cfg(not(feature = "event-dispatcher"))]
        if dispatcher_enabled(candidate) {
            return Err(
                "`api.event_sinks`, `api.dead_letter_path`, and `api.outbox_path` require Cargo feature `event-dispatcher`"
                    .to_owned(),
            );
        }

        Ok(())
    }

    async fn prepare_carrier_budget(
        &self,
        current: &RuntimeConfig,
        target: Arc<RuntimeConfig>,
    ) -> Result<Option<ConfigReconcileResult>, String> {
        if current.runtime.effective_carrier_max_total_originals()
            == target.runtime.effective_carrier_max_total_originals()
            || !(dispatcher_enabled(current) || dispatcher_enabled(&target))
        {
            return Ok(None);
        }
        // The existing dispatcher is prepared paused: old native/task custody
        // returns before the same Host total changes, while target delivery
        // cannot acquire a new native original until the real host commit.
        self.reconcile(target).await.map(Some)
    }

    async fn reconcile(&self, target: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        let mut state = self.state.lock().await;
        if let Some(attempt) = &state.attempt {
            if attempt.target.as_ref() != target.as_ref() {
                if self.engine.config().as_ref() == attempt.target.as_ref() {
                    self.publish_prepared_hooks(&mut state);
                    #[cfg(feature = "event-dispatcher")]
                    if let Some(dispatcher) = state.dispatcher.as_mut() {
                        if let Err(error) = dispatcher.activate() {
                            let message = error.to_string();
                            state.cleanup_fault.get_or_insert_with(|| Arc::new(error));
                            return Err(message);
                        }
                    }
                    if let Some(candidate) = state
                        .attempt
                        .as_ref()
                        .and_then(|attempt| attempt.control_budget.as_ref())
                    {
                        candidate.commit().map_err(|error| error.to_string())?;
                    }
                    state.attempt = None;
                } else {
                    return Err(
                        "another application attempt still requires confirmed retirement"
                            .to_owned(),
                    );
                }
            }
        }
        state.attempt.get_or_insert_with(|| ApplicationAttempt {
            target: target.clone(),
            dispatcher_touched: false,
            hooks_touched: false,
            control_budget: None,
        });
        let limits =
            zero_runtime::configuration::ControlBudgetLimits::from_runtime(&target.runtime);
        let attempt = state.attempt.as_mut().unwrap();
        if attempt.control_budget.is_none() && self.control_budget.limits() != limits {
            attempt.control_budget = Some(
                self.control_budget
                    .prepare(limits)
                    .map_err(|error| error.to_string())?,
            );
        }
        self.install_target(&mut state, target).await
    }

    async fn retire_candidate(&self, target: Arc<RuntimeConfig>) -> Result<(), String> {
        let mut state = self.state.lock().await;
        let Some(attempt) = &state.attempt else {
            return Ok(());
        };
        if attempt.target.as_ref() != target.as_ref() {
            return Err("application retirement differs from its actual prepared attempt".into());
        }
        let touched = attempt.dispatcher_touched;
        let hooks_touched = attempt.hooks_touched;
        #[cfg(feature = "event-dispatcher")]
        let dispatcher = if touched {
            self.stop_active_services(&mut state).await
        } else {
            Ok(())
        };
        #[cfg(not(feature = "event-dispatcher"))]
        let dispatcher = {
            let _ = touched;
            Ok::<(), String>(())
        };
        let hooks = if hooks_touched {
            self.retire_hooks(&mut state)
        } else {
            Ok(())
        };
        dispatcher?;
        hooks?;
        if let Some(error) = &state.cleanup_fault {
            return Err(format!(
                "original application cleanup is unresolved: {error}"
            ));
        }
        if let Some(candidate) = state
            .attempt
            .as_ref()
            .and_then(|attempt| attempt.control_budget.as_ref())
        {
            candidate.retire().map_err(|error| error.to_string())?;
        }
        state.attempt = None;
        Ok(())
    }

    async fn committed(&self, target: Arc<RuntimeConfig>) -> Result<(), String> {
        let mut state = self.state.lock().await;
        if state
            .attempt
            .as_ref()
            .is_some_and(|attempt| attempt.target.as_ref() != target.as_ref())
        {
            return Err("application commit differs from its actual prepared attempt".to_owned());
        }
        self.publish_prepared_hooks(&mut state);
        #[cfg(feature = "event-dispatcher")]
        if let Some(dispatcher) = state.dispatcher.as_mut() {
            if let Err(error) = dispatcher.activate() {
                let message = error.to_string();
                state.cleanup_fault.get_or_insert_with(|| Arc::new(error));
                return Err(message);
            }
        }
        if let Some(candidate) = state
            .attempt
            .as_ref()
            .and_then(|attempt| attempt.control_budget.as_ref())
        {
            candidate.commit().map_err(|error| error.to_string())?;
        }
        state.attempt = None;
        Ok(())
    }
}
