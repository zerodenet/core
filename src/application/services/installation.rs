use super::*;
impl ApplicationServices {
    pub(super) async fn install_target(
        &self,
        state: &mut ApplicationServiceState,
        target: Arc<RuntimeConfig>,
    ) -> Result<ConfigReconcileResult, String> {
        if let Some(error) = &state.cleanup_fault {
            return Err(format!(
                "original application cleanup is unresolved: {error}"
            ));
        }
        if state.installed
            && state.config.as_ref() == target.as_ref()
            && self.services_match_target(state, &target)
        {
            return Ok(ConfigReconcileResult::default());
        }

        let previous = state.config.clone();
        let components = changed_components(previous.as_ref(), target.as_ref());
        let rebuild_hooks = !state.installed || previous.api.hooks != target.api.hooks;
        #[cfg(feature = "event-dispatcher")]
        let rebuild_dispatcher = !state.installed
            || dispatcher_changed(&previous, &target)
            || !self.services_match_target(state, &target);
        #[cfg(feature = "event-dispatcher")]
        if rebuild_dispatcher {
            if let Some(attempt) = &mut state.attempt {
                attempt.dispatcher_touched = true;
            }
            self.stop_active_services(state).await?;
        }
        state.config = target.clone();
        state.installed = false;

        if rebuild_hooks {
            let warning_handler = {
                let engine = self.engine.clone();
                Some(Arc::new(move |code: &str, message: &str| {
                    engine.emit_warning(code, message);
                }) as Arc<dyn Fn(&str, &str) + Send + Sync>)
            };
            let hook_chain = hooks::build_hook_chain(
                self.ipc_hook_socket.as_deref(),
                &target.api,
                warning_handler,
            );
            state.pending_hooks = Some((!hook_chain.is_empty()).then_some(hook_chain));
        }

        #[cfg(feature = "event-dispatcher")]
        if rebuild_dispatcher {
            let has_delivery_sinks = !target.api.event_sinks.is_empty();
            let bootstrap_engine_started = has_delivery_sinks && !state.engine_started_bootstrapped;
            self.event_network
                .begin_generation()
                .map_err(|error| error.to_string())?;
            state.network_generation_started = true;
            state.dispatcher = Some(
                zero_connector::prepare_event_dispatcher_paused_with_network(
                    self.engine.clone(),
                    target.api.clone(),
                    target.source_dir.clone(),
                    zero_connector::EventDispatcherOptions::default(),
                    self.event_network.clone(),
                    bootstrap_engine_started,
                    state.continuation.clone(),
                ),
            );
            #[cfg(test)]
            if let Some(pause) = state.preparation_pause.take() {
                pause.prepared.notify_one();
                pause
                    .resume
                    .acquire()
                    .await
                    .expect("test initialization gate")
                    .forget();
            }
            // The actual original worker is installed before this readiness await.
            let ready = state
                .dispatcher
                .as_mut()
                .expect("installed dispatcher owner")
                .wait_ready()
                .await
                .map_err(|error| error.to_string())?;
            if !ready {
                state
                    .dispatcher
                    .as_mut()
                    .unwrap()
                    .retire_prepared_retained()
                    .await
                    .map_err(|error| error.to_string())?;
                self.event_network
                    .retire_generation(
                        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                state.network_generation_started = false;
                state.dispatcher = None;
            }
            if state.dispatcher.is_some() {
                state.continuation = None;
            }
            if bootstrap_engine_started && state.dispatcher.is_some() {
                state.engine_started_bootstrapped = true;
            }
            state.monitor_exit = None;
            state.status_monitor = Some(spawn_status_monitor(
                self.engine.clone(),
                state.dispatcher.as_ref(),
            ));
        }

        state.installed = true;
        Ok(ConfigReconcileResult { components })
    }

    pub(super) fn services_match_target(
        &self,
        _state: &ApplicationServiceState,
        _target: &RuntimeConfig,
    ) -> bool {
        #[cfg(feature = "event-dispatcher")]
        if _state
            .dispatcher
            .as_ref()
            .is_some_and(|owner| owner.is_terminated())
        {
            return false;
        }
        #[cfg(feature = "event-dispatcher")]
        if _state.dispatcher.is_some() != dispatcher_enabled(_target) {
            return false;
        }
        true
    }
}
