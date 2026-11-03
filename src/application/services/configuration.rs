use super::*;

#[async_trait::async_trait]
impl ConfigApplyReconciler for ApplicationServices {
    fn validate(&self, current: &RuntimeConfig, candidate: &RuntimeConfig) -> Result<(), String> {
        // This executes before Proxy/DNS prepare. An unresolved application
        // original cannot authorize another candidate in a different component.
        let state = self.state.try_lock().map_err(|_| {
            "application preparation/retirement is still owned by its current operation".to_owned()
        })?;
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
        });
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
        #[cfg(feature = "event-dispatcher")]
        if touched {
            self.stop_active_services(&mut state).await?;
        }
        #[cfg(not(feature = "event-dispatcher"))]
        let _ = touched;
        if let Some(error) = &state.cleanup_fault {
            return Err(format!(
                "original application cleanup is unresolved: {error}"
            ));
        }
        state.pending_hooks = None; // cold values; no native/task receipt is inferred
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
        state.attempt = None;
        Ok(())
    }
}
