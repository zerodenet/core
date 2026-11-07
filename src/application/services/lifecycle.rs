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
                let outcome = dispatcher.shutdown_retained().await;
                let complete = dispatcher.retirement_complete();
                if let Err(error) = outcome {
                    state.remember_cleanup_failure(
                        if complete {
                            CleanupOwner::DispatcherClosed
                        } else {
                            CleanupOwner::DispatcherPending
                        },
                        Arc::new(error),
                    );
                }
                if complete {
                    state.dispatcher = None;
                }
            }
            if state.network_generation_started {
                let outcome = self
                    .event_network
                    .retire_generation(
                        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                    )
                    .await;
                let complete = self.event_network.retirement_complete();
                if let Err(error) = outcome {
                    state.remember_cleanup_failure(
                        if complete {
                            CleanupOwner::NetworkClosed
                        } else {
                            CleanupOwner::NetworkPending
                        },
                        Arc::new(error),
                    );
                }
                if complete {
                    state.network_generation_started = false;
                }
            }
        }
        if let Some(error) = state.cleanup_error() {
            return Err(std::io::Error::other(error));
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
            let error = error.clone();
            state.remember_cleanup_failure(CleanupOwner::Monitor, error.clone());
            return Err(error);
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
        let mut failures = [None, None];
        for (index, hooks) in [&mut state.pending_hooks, &mut state.hooks]
            .into_iter()
            .enumerate()
        {
            if let Some(originals) = hooks.as_mut() {
                match originals.retire() {
                    Ok(()) => *hooks = None,
                    Err(error) => failures[index] = Some(Arc::new(error)),
                }
            }
        }
        for (owner, error) in [CleanupOwner::PreparedHooks, CleanupOwner::Hooks]
            .into_iter()
            .zip(failures)
        {
            if let Some(error) = error {
                state.remember_cleanup_failure(owner, error);
            }
        }
        if let Some(error) = &state.cleanup_fault {
            return Err(format!(
                "original application cleanup remains unresolved: {error}"
            ));
        }
        Ok(())
    }

    /// Read actual retirement separately from the retained failure returned to
    /// the supervisor. An observed panic must not become permanent join debt.
    pub(in crate::application) async fn service_retirement_complete(&self) -> bool {
        let state = self.state.lock().await;
        if state.hooks.is_some() || state.pending_hooks.is_some() {
            return false;
        }
        #[cfg(feature = "event-dispatcher")]
        if state.status_monitor.is_some()
            || state.dispatcher.is_some()
            || state.network_generation_started
        {
            return false;
        }
        true
    }

    #[cfg(feature = "event-dispatcher")]
    pub(super) async fn stop_active_services(
        &self,
        _state: &mut ApplicationServiceState,
    ) -> Result<(), String> {
        let monitor = Self::stop_status_monitor(_state).await;
        let dispatcher = if let Some(dispatcher) = _state.dispatcher.as_mut() {
            _state.installed = false;
            let outcome = dispatcher.retire_prepared_retained().await;
            let complete = dispatcher.retirement_complete();
            let result = match outcome {
                Ok(continuation) if complete => {
                    if let Some(continuation) = continuation {
                        _state.continuation = Some(continuation);
                    }
                    Ok(())
                }
                Ok(_) => {
                    Err("actual dispatcher original retirement remains unconfirmed".to_owned())
                }
                Err(error) => {
                    let reported_preparation = complete
                        && _state
                            .attempt
                            .as_ref()
                            .and_then(|attempt| attempt.reported_dispatcher_preparation.as_ref())
                            .is_some_and(|original| {
                                matches!(&error,
                            zero_connector::ConnectorError::DispatcherRetirement(failure)
                                if failure.is_only_preparation(original))
                            });
                    if reported_preparation {
                        _state.remember_reported_dispatcher_failure(Arc::new(error));
                        Ok(())
                    } else {
                        let message = error.to_string();
                        _state.remember_cleanup_failure(
                            if complete {
                                CleanupOwner::DispatcherClosed
                            } else {
                                CleanupOwner::DispatcherPending
                            },
                            Arc::new(error),
                        );
                        Err(message)
                    }
                }
            };
            if complete {
                _state.dispatcher = None;
            }
            result
        } else {
            Ok(())
        };
        if _state.network_generation_started {
            let outcome = self
                .event_network
                .retire_generation(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
                .await;
            let complete = self.event_network.retirement_complete();
            if let Err(error) = outcome {
                _state.remember_cleanup_failure(
                    if complete {
                        CleanupOwner::NetworkClosed
                    } else {
                        CleanupOwner::NetworkPending
                    },
                    Arc::new(error),
                );
            }
            if complete {
                _state.network_generation_started = false;
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
