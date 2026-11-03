use super::*;
impl ApplicationServices {
    pub(super) async fn shutdown_status_monitor(&self) -> Result<(), String> {
        #[cfg(feature = "event-dispatcher")]
        {
            let mut state = self.state.lock().await;
            Self::stop_status_monitor(&mut state).await?;
        }
        Ok(())
    }

    pub(super) async fn shutdown_dispatcher(&self) -> Result<(), String> {
        #[cfg(feature = "event-dispatcher")]
        {
            let mut state = self.state.lock().await;
            if let Some(dispatcher) = state.dispatcher.as_mut() {
                if let Err(error) = dispatcher.shutdown_retained().await {
                    state.cleanup_fault = Some(Arc::new(error));
                    return Err("original dispatcher shutdown remains unconfirmed".to_owned());
                }
                state.dispatcher = None;
            }
            if state.network_generation_started {
                if let Err(error) = self
                    .event_network
                    .retire_generation(
                        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                    )
                    .await
                {
                    state.cleanup_fault = Some(Arc::new(error));
                    return Err(
                        "original application native cleanup remains unconfirmed".to_owned()
                    );
                }
                state.network_generation_started = false;
            }
        }
        self.engine.update_sink_status(Vec::new());
        Ok(())
    }

    #[cfg(feature = "event-dispatcher")]
    pub(super) async fn stop_status_monitor(
        state: &mut ApplicationServiceState,
    ) -> Result<(), String> {
        if state.monitor_exit.is_none() {
            if let Some(monitor) = state.status_monitor.as_mut() {
                monitor.abort(); // request only; the actual original join follows
                state.monitor_exit = Some((&mut *monitor).await.map_err(Arc::new));
                state.status_monitor = None;
            }
        }
        if let Some(Err(error)) = &state.monitor_exit {
            if !error.is_cancelled() {
                state.cleanup_fault = Some(error.clone());
                return Err(format!("original status monitor failed: {error}"));
            }
        }
        Ok(())
    }

    pub(super) fn publish_prepared_hooks(&self, state: &mut ApplicationServiceState) {
        if let Some(hooks) = state.pending_hooks.take() {
            self.engine.replace_flow_hook_chain(hooks);
        }
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
                    _state.cleanup_fault = Some(Arc::new(error));
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
                Err(error) => _state.cleanup_fault = Some(Arc::new(error)),
            }
        }
        monitor?;
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
