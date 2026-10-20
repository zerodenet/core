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
        self.scope.begin_shutdown();
        let shared = &self.shared;
        let scope = &mut self.scope;
        let task_report = &mut self.report;
        let failures = &mut self.failures;
        let io_report = &mut self.io_report;
        let join_tasks = async {
            let mut tasks = scope.shutdown(deadline).await;
            for failure in tasks.failed_tasks.drain(..) {
                let source = Arc::new(failure.error);
                shared
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .failure = Some(source.clone());
                failures.push((failure.id, failure.class, source));
            }
            let unfinished = std::mem::take(&mut tasks.unfinished);
            task_report.append(tasks);
            unfinished
        };
        let close_io = async {
            let Ok(mut sockets) = timeout_at(deadline, shared.sockets.lock()).await else {
                return Ok(vec![Unfinished::Resource(
                    "Direct original socket I/O owner still holds state".into(),
                )]);
            };
            // This is the same guard held throughout the unique driver's I/O
            // loop. Scope cancellation forbids any resumed/new driver I/O.
            // Joining all scope tasks first would deadlock a task waiting for
            // this actual association's mapping/queue cleanup.
            sockets.close_all_native();
            let mut native = sockets.take_native_close_report();
            let unfinished = std::mem::take(&mut native.unfinished);
            io_report.append(native); // Persist before any error/next suspension.
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (_, entry) in std::mem::take(&mut state.sessions) {
                entry.logical.drain_receive_queue();
                entry.logical.closed.store(true, Ordering::Release);
                entry.logical.changed.notify_waiters();
            }
            if let Some((_id, source)) = sockets.native_close_failure() {
                return Err(
                    error(Stage::Close, Reason::Io, DeliveryState::Unknown).with_source(source)
                );
            }
            Ok::<_, ChannelError>(unfinished)
        };
        // Each branch commits its real receipts into owner fields immediately.
        // Cancelling this join cannot discard the other branch's completed work.
        let (mut unfinished, native) = tokio::join!(join_tasks, close_io);
        unfinished.extend(native?);
        let mut report = std::mem::take(&mut self.report);
        report.append(std::mem::take(&mut self.io_report));
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
