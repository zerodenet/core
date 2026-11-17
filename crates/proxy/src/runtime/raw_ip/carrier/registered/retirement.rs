//! Return unique views before settling every actual attempt of this wire.
use super::*;
use zero_platform_tokio::contract::lifecycle::CloseReport;

impl State {
    pub(super) async fn retire_path(
        &self,
        path: &Arc<WirePath>,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        let logical = path
            .carrier
            .retire_driver_handoff(&self.wire, deadline)
            .await;
        // Attempt records retain controllers even if handoff or logical view
        // retirement failed. Continue their real native/prefix phases as well.
        let originals = self.execution.retire_carriers(deadline).await;
        let mut report = CloseReport::default();
        let mut first = None;
        for result in [logical, originals] {
            match result {
                Ok(closed) => report.append(closed),
                Err(source) => {
                    let source = Arc::new(source);
                    self.execution.receive_cleanup_failure(source.clone())?;
                    first.get_or_insert(source);
                }
            }
        }
        match first {
            Some(source) => Err(ChannelError::retained(source)),
            None => Ok(report),
        }
    }

    pub(super) async fn close_originals(&self) -> Result<(), EngineError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        self.stop();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let background = tokio::time::timeout_at(deadline, self.wait_background())
            .await
            .map_err(|source| {
                EngineError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, source))
            })
            .and_then(|result| result);
        let current = self.path.borrow().clone();
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut report = CloseReport::default();
        let mut first = None;
        // Return ALL installed views before driving their original attempt
        // book. A not-yet-published candidate may still own a distinct view;
        // waiting on its provider first would wait on our own retained input.
        for path in current.into_iter().chain(pending) {
            match path
                .carrier
                .retire_driver_handoff(&self.wire, deadline)
                .await
            {
                Ok(closed) => report.append(closed),
                Err(source) => {
                    let source = Arc::new(source);
                    let reception = self.execution.receive_cleanup_failure(source.clone());
                    first.get_or_insert_with(|| ChannelError::retained(source));
                    if let Err(source) = reception {
                        first.get_or_insert(source);
                    }
                }
            }
        }
        match self.execution.retire_carriers(deadline).await {
            Ok(closed) => report.append(closed),
            Err(source) => {
                let source = Arc::new(source);
                let reception = self.execution.receive_cleanup_failure(source.clone());
                first.get_or_insert_with(|| ChannelError::retained(source));
                if let Err(source) = reception {
                    first.get_or_insert(source);
                }
            }
        }
        // Actual task failures never skip independent original cleanup above.
        // They remain failures after every provider has had a chance to close.
        if let Err(source) = background {
            let received = self.request_failure.get().is_none()
                && self.execution.cleanup_failures_received()
                && match self.actor() {
                    Actor::Registered(witness) => witness.try_joined().is_some_and(|joined| {
                        witness.matches_joined(&joined)
                            && joined.registration_released()
                            && joined.result().is_err()
                            && joined.failure_accepted()
                    }),
                    _ => false,
                };
            if !received {
                return Err(source);
            }
            // Its actual error remains in this Source's canonical native
            // report/archive. A received task fault cannot turn matching
            // positive original-close receipts into an infinite cleanup debt.
        }
        let joined = tokio::time::timeout_at(deadline, self.wire.wait_registration_released())
            .await
            .map_err(|source| {
                EngineError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, source))
            })?;
        if !self.wire.matches_joined(&joined) || !joined.registration_released() {
            return Err(failed(broken("same outer wire has not actually joined")));
        }
        if let Some(source) = first {
            return Err(failed(source));
        }
        if !report.is_complete() {
            return Err(failed(broken(
                "same outer wire retains original carrier debt",
            )));
        }
        // This call's controls, exclusive originals and returned donor custody
        // are settled. Unchanged shared pools remain live under their owners;
        // replacing them still consumes their independent Host native receipts.
        tracing::debug!(
            wire = ?self.wire.task_id(),
            exited_tasks = report.exited_tasks.len(),
            closed_io = report.closed_io.len(),
            released_resources = report.released_resources.len(),
            "raw-IP outer originals closed and wire registration released"
        );
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}
