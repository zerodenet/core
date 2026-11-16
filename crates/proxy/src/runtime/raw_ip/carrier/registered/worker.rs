//! One real Scope actor retires originals before any replacement acquisition.
use super::*;
impl State {
    pub(super) async fn rebuild(
        &self,
        failed_path: Arc<WirePath>,
        execution: PacketPathExecutionSource,
    ) -> Result<(), ChannelError> {
        let mut closing = self.closing.subscribe();
        let mut backoff = Duration::from_secs(1);
        loop {
            if self.stopped() || !self.is_current(&failed_path) {
                return Ok(());
            }
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            // A superseded, already acquired candidate remains strongly owned
            // until its own original views/controllers have actually returned.
            let pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let mut can_build = true;
            if let Some(pending) = pending {
                can_build = tokio::select! {
                    result = self.retire_for_retry(&pending, deadline) => result,
                    _ = closing.changed() => return Ok(()),
                    _ = execution.cancelled() => return Ok(()),
                };
                if can_build {
                    let removed = {
                        let mut actual = self
                            .pending
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if actual
                            .as_ref()
                            .is_some_and(|same| Arc::ptr_eq(same, &pending))
                        {
                            actual.take()
                        } else {
                            None
                        }
                    };
                    drop(removed);
                }
            }
            if can_build {
                can_build = tokio::select! {
                    result = self.retire_for_retry(&failed_path, deadline) => result,
                    _ = closing.changed() => return Ok(()),
                    _ = execution.cancelled() => return Ok(()),
                };
            }
            if can_build && !self.stopped() {
                let endpoint = *self
                    .desired
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let built = tokio::select! {
                    result = execution.build_carrier(&self.recipe, endpoint) => result,
                    _ = closing.changed() => return Ok(()),
                    _ = execution.cancelled() => return Ok(()),
                };
                match built {
                    Ok(carrier) => {
                        let next = Arc::new(WirePath { carrier, endpoint });
                        *self
                            .pending
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            Some(next.clone());
                        next.carrier.confirm_driver_handoff(&self.wire)?;
                        if self.stopped() {
                            return Ok(());
                        }
                        if *self
                            .desired
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            != endpoint
                        {
                            // Endpoint changed while setup was suspended. The
                            // loop returns this candidate before building again.
                            continue;
                        }
                        self.path.send_replace(Some(next));
                        let installed = self
                            .pending
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        drop(installed);
                        return Ok(());
                    }
                    Err(source) => {
                        tracing::warn!(?source, "raw-IP outer packet path reconnect failed")
                    }
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {},
                _ = closing.changed() => return Ok(()),
                _ = execution.cancelled() => return Ok(()),
            }
            backoff = (backoff * 2).min(Duration::from_secs(15));
        }
    }
    async fn retire_for_retry(&self, path: &Arc<WirePath>, deadline: tokio::time::Instant) -> bool {
        match self.retire_path(path, deadline).await {
            Ok(report) => report.is_complete(),
            Err(source) => {
                // The exact issuer report is already retained/forwarded by
                // retire_path. Logging cannot authorize the following build.
                tracing::warn!(?source, "same outer path original retirement retained");
                false
            }
        }
    }
    pub(super) async fn wait_background(&self) -> Result<(), EngineError> {
        // Initial preparation is owned before it can suspend, too. Returning
        // from an actor-only join cannot prove that its acquisition has ended.
        let _initialization = self.initialization.lock().await;
        let result = match self.actor() {
            Actor::Vacant => Ok(()),
            Actor::Registering => Err(failed(broken(
                "same outer reconnect registration is unresolved",
            ))),
            Actor::Registered(witness) => {
                let joined = witness.wait().await;
                if joined.result().is_err() {
                    // Persist the actual joined error BEFORE awaiting its Host
                    // registration ACK. The provider may already be waiting on
                    // this owner's still-retained views; delayed acceptance
                    // until after provider cleanup would form a real cycle.
                    self.execution
                        .receive_reconnect_failures()
                        .map_err(failed)?;
                }
                // Only the real Scope consumes the one JoinHandle and Host
                // ACK. This waiter cannot synthesize either or refund its task.
                let joined = witness.wait_registration_released().await;
                joined
                    .result()
                    .map_err(|source| failed(ChannelError::retained(source)))
            }
        };
        result?;
        match self.request_failure.get() {
            Some(source) => Err(failed(ChannelError::retained(source.clone()))),
            None => Ok(()),
        }
    }
}
