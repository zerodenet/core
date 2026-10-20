use super::*;
use std::sync::atomic::Ordering;
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle, TaskClass, TaskFailure, Unfinished},
    CloseReason,
};
impl std::fmt::Debug for DirectEndpointOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f.debug_struct("DirectEndpointOwner")
            .field("identity", &self.shared.identity)
            .field("started", &state.started)
            .field("closing", &state.closing)
            .field("active_associations", &state.sessions.len())
            .field("failed", &state.failure.is_some())
            .finish()
    }
}
impl ResourceLifecycle for DirectEndpointOwner {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closing || self.shared.scope_cancel.is_cancelled() {
            return Err(error(
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        if state.started {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(error(
                Stage::Prepare,
                Reason::Timeout,
                DeliveryState::NotSubmitted,
            ));
        }
        let receiver = self.commands.take().ok_or_else(|| {
            error(
                Stage::Prepare,
                Reason::Internal,
                DeliveryState::NotSubmitted,
            )
        })?;
        let shared = self.shared.clone();
        match self
            .scope
            .registrar()
            .spawn(TaskClass::Critical, driver::run(shared, receiver))
        {
            Ok(_) => {
                state.started = true;
                Ok(())
            }
            Err(source) => {
                let failure = Arc::new(
                    error(
                        Stage::Prepare,
                        Reason::Capacity,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(source),
                );
                state.failure = Some(failure.clone());
                state.closing = true;
                Err(error(
                    Stage::Prepare,
                    Reason::Capacity,
                    DeliveryState::NotSubmitted,
                )
                .with_source(failure))
            }
        }
    }
    fn revoke(&self, _reason: CloseReason) {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closing = true;
        for entry in state.sessions.values() {
            entry.logical.cancel.cancel();
        }
        drop(state);
        self.shared.changed.notify_one();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        // TaskScope retains its actual JoinSet and partial exits if this await
        // is interrupted. The same native state remains owner-held throughout.
        let mut tasks = self.scope.shutdown(deadline).await;
        for failure in tasks.failed_tasks.drain(..) {
            let source = Arc::new(failure.error);
            self.shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .failure = Some(source.clone());
            self.failures.push((failure.id, failure.class, source));
        }
        let mut unfinished = std::mem::take(&mut tasks.unfinished);
        self.report.append(tasks); // Persist receipts before the next suspension.
        if unfinished.is_empty() {
            match timeout_at(deadline, self.shared.sockets.lock()).await {
                Ok(mut sockets) => {
                    sockets.close_all_native();
                    let mut native = sockets.take_native_close_report();
                    unfinished.append(&mut native.unfinished);
                    self.report.append(native); // Persist before returning an error.
                    if let Some((_id, source)) = sockets.native_close_failure() {
                        return Err(error(Stage::Close, Reason::Io, DeliveryState::Unknown)
                            .with_source(source));
                    }
                    let mut state = self
                        .shared
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    for (_, entry) in std::mem::take(&mut state.sessions) {
                        entry.logical.closed.store(true, Ordering::Release);
                        entry.logical.changed.notify_waiters();
                    }
                }
                Err(_) => unfinished.push(Unfinished::Resource(
                    "Direct original socket state acquisition after task exit".into(),
                )),
            }
        }
        let mut report = std::mem::take(&mut self.report);
        report.unfinished = unfinished;
        report
            .failed_tasks
            .extend(self.failures.iter().map(|(id, class, source)| {
                TaskFailure {
                    id: *id,
                    class: *class,
                    error: error(Stage::Close, Reason::Internal, DeliveryState::Unknown)
                        .with_source(source.clone()),
                }
            }));
        Ok(report)
    }
}
