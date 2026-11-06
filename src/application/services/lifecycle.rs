use super::*;
impl ApplicationServices {
    pub(in crate::application) fn install_parent_input(
        self: &Arc<Self>,
    ) -> std::io::Result<Arc<zero_platform_tokio::parent_input::ParentInputOwner>> {
        APPLICATION_STARTUP.retain(self.clone())?;
        Ok(self
            .parent_input
            .get_or_init(zero_platform_tokio::parent_input::ParentInputOwner::new)
            .clone())
    }
    pub(in crate::application) async fn shutdown_parent_input(&self) -> std::io::Result<()> {
        let Some(owner) = self.parent_input.get() else {
            return Ok(());
        };
        let report = owner
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
            .await?;
        if report.is_complete() {
            Ok(())
        } else {
            Err(std::io::Error::other(format!(
                "same parent input cleanup remains retained: {report:?}"
            )))
        }
    }
    pub(in crate::application) fn parent_input_retirement_complete(&self) -> bool {
        self.parent_input
            .get()
            .is_none_or(|owner| owner.retirement_complete())
    }

    pub(in crate::application) async fn shutdown_status_monitor(&self) -> std::io::Result<()> {
        #[cfg(feature = "event-dispatcher")]
        {
            let mut state = self.state.lock().await;
            Self::stop_status_monitor(&mut state)
                .await
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }

    pub(in crate::application) async fn shutdown_dispatcher(&self) -> std::io::Result<()> {
        let mut state = self.state.lock().await;
        let hooks = self.retire_hooks(&mut state);
        // The fenced callbacks have returned; retained concrete state still
        // owns every unresolved original. Uninstall the application chain to
        // release its Engine warning-handler cycle during final shutdown.
        self.engine.replace_flow_hook_chain(None);
        #[cfg(feature = "event-dispatcher")]
        {
            if let Some(dispatcher) = state.dispatcher.as_mut() {
                match dispatcher.shutdown_retained().await {
                    Ok(()) => state.dispatcher = None,
                    Err(error) => {
                        state.cleanup_fault.get_or_insert_with(|| Arc::new(error));
                    }
                }
            }
            if state.network_generation_started {
                match self
                    .event_network
                    .retire_generation(
                        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                    )
                    .await
                {
                    Ok(()) => state.network_generation_started = false,
                    Err(error) => {
                        state.cleanup_fault.get_or_insert_with(|| Arc::new(error));
                    }
                }
            }
        }
        if let Some(error) = &state.cleanup_fault {
            return Err(std::io::Error::other(error.clone()));
        }
        hooks.map_err(std::io::Error::other)?;
        self.engine.update_sink_status(Vec::new());
        Ok(())
    }

    #[cfg(feature = "event-dispatcher")]
    pub(super) async fn stop_status_monitor(
        state: &mut ApplicationServiceState,
    ) -> Result<(), Arc<zero_platform_tokio::contract::ChannelError>> {
        if let Some(monitor) = state.status_monitor.as_ref() {
            let outcome = monitor.shutdown().await;
            if !monitor.retirement_complete() {
                // Retain the exact scope and retry its unfinished joins. A
                // timeout is not the worker's terminal fault or close receipt.
                return outcome;
            }
            state.monitor_exit = Some(outcome);
            state.status_monitor = None;
        }
        if let Some(Err(error)) = &state.monitor_exit {
            state.cleanup_fault.get_or_insert_with(|| error.clone());
            return Err(error.clone());
        }
        Ok(())
    }

    pub(super) fn publish_prepared_hooks(&self, state: &mut ApplicationServiceState) {
        if let Some(mut hooks) = state.pending_hooks.take() {
            self.engine.replace_flow_hook_chain(hooks.take_chain());
            state.hooks = Some(hooks);
        }
    }

    pub(super) fn retire_hooks(&self, state: &mut ApplicationServiceState) -> Result<(), String> {
        for hooks in [&mut state.pending_hooks, &mut state.hooks] {
            if let Some(originals) = hooks.as_mut() {
                if let Err(error) = originals.retire() {
                    state.cleanup_fault.get_or_insert_with(|| Arc::new(error));
                }
            }
        }
        if let Some(error) = &state.cleanup_fault {
            return Err(format!(
                "original application cleanup remains unresolved: {error}"
            ));
        }
        state.pending_hooks = None;
        state.hooks = None;
        Ok(())
    }

    #[cfg(feature = "event-dispatcher")]
    pub(super) async fn stop_active_services(
        &self,
        _state: &mut ApplicationServiceState,
    ) -> Result<(), String> {
        let monitor = Self::stop_status_monitor(_state).await;
        let dispatcher = if let Some(dispatcher) = _state.dispatcher.as_mut() {
            _state.installed = false;
            match dispatcher.retire_prepared_retained().await {
                Ok(continuation) => {
                    if let Some(continuation) = continuation {
                        _state.continuation = Some(continuation);
                    }
                    _state.dispatcher = None;
                    Ok(())
                }
                Err(error) => {
                    let message = error.to_string();
                    _state.cleanup_fault.get_or_insert_with(|| Arc::new(error));
                    Err(message)
                }
            }
        } else {
            Ok(())
        };
        if _state.network_generation_started {
            match self
                .event_network
                .retire_generation(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
                .await
            {
                Ok(()) => _state.network_generation_started = false,
                Err(error) => {
                    _state.cleanup_fault.get_or_insert_with(|| Arc::new(error));
                }
            }
        }
        monitor.map_err(|error| format!("original status monitor failed: {error}"))?;
        dispatcher?;
        if let Some(error) = &_state.cleanup_fault {
            return Err(format!(
                "original application cleanup is unresolved: {error}"
            ));
        }
        self.engine.update_sink_status(Vec::new());
        Ok(())
    }
}
