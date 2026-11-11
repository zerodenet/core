//! Stop requests, actual worker joins and original-file close are separate gates.
use super::*;

impl Execution {
    pub(in crate::configuration::source::quota) fn retired(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        !state.creating
            && !state.polling
            && state
                .standard
                .as_ref()
                .is_none_or(|worker| worker.original.is_none() && worker.joined.is_some())
            && state.scoped.as_ref().is_none_or(scoped::Actor::drained)
            && (state.producer.is_none()
                || (state.stopping && (state.final_done || state.terminal.is_some())))
    }
    pub(super) fn receive_task_failure(&self, failure: QuotaWriterFailure) -> Arc<io::Error> {
        let original = Arc::new(io::Error::other(failure));
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.terminal.get_or_insert_with(|| original.clone());
        drop(state);
        self.changed.notify_waiters();
        original
    }

    pub(super) fn receive_driver_failure(&self, error: io::Error) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        // The first original fault remains strongly held. Repeated observation
        // never replaces its actual task receipt or native error source.
        state.terminal.get_or_insert_with(|| Arc::new(error));
        drop(state);
        self.changed.notify_waiters();
    }
}

impl OwnedQuotaStorage {
    pub fn writer_error(&self) -> Option<Arc<io::Error>> {
        let state = self
            .execution
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.terminal.clone().or_else(|| state.last_error.clone())
    }

    pub fn request_writer_stop(&self) {
        let signal = {
            let mut state = self
                .execution
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.stopping = true;
            state.standard.as_ref().map(|worker| worker.signal.clone())
        };
        if let Some(signal) = signal {
            signal.shutdown();
        }
        self.execution.changed.notify_waiters();
    }

    /// Read-only retirement predicate. A request, completed flush or ACK alone
    /// cannot stand in for the real thread/Scope and matching original close.
    pub fn writer_retirement_complete(&self) -> bool {
        self.execution.retired()
            && self.confirm_closed().is_ok()
            && self.original_io_status().is_ok()
    }

    fn shutdown_result(&self, reported: bool) -> io::Result<()> {
        if !self.writer_retirement_complete() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "quota original writer retirement remains pending",
            ));
        }
        if let Some(original) = self.retained_worker_failure() {
            self.receive_worker_failure(original.clone(), reported)?;
            return Err(retained(&original));
        }
        self.writer_error()
            .map_or(Ok(()), |error| Err(retained(&error)))
    }

    /// Genuine Engine-only owners join their original std worker synchronously.
    /// Host-bound callers must use the asynchronous path; never block its runtime
    /// waiting for a task that the same caller needs to poll/acknowledge.
    pub fn shutdown_writer(&self, reported: bool) -> io::Result<()> {
        self.request_writer_stop();
        loop {
            let hosted = self
                .execution
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .tasks
                .is_some();
            if hosted {
                return self.shutdown_result(reported);
            }
            let mut cx = Context::from_waker(std::task::Waker::noop());
            if let Err(error) = self.poll_writer(&mut cx) {
                self.execution.receive_driver_failure(error);
            }
            if self.writer_retirement_complete() {
                return self.shutdown_result(reported);
            }
            if let Some(error) = self.writer_error() {
                // An unknown native disposition cannot become a close receipt
                // by waiting. Retain this Source rather than dropping its owner.
                if self.execution.retired() && self.original_io_status().is_err() {
                    return Err(retained(&error));
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub async fn shutdown_writer_async(&self) -> io::Result<()> {
        self.request_writer_stop();
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let result = std::future::poll_fn(|cx| Poll::Ready(self.poll_writer(cx))).await;
            if let Err(error) = result {
                self.execution.receive_driver_failure(error);
            }
            if self.writer_retirement_complete() {
                return self.shutdown_result(true);
            }
            if self.execution.retired() && self.original_io_status().is_err() {
                return self
                    .writer_error()
                    .map_or_else(|| self.original_io_status(), |error| Err(retained(&error)));
            }
            // Cancelling this wait leaves the actual actors and their receipts
            // installed in Source. No local future owns or detaches a worker.
            tokio::select! {
                _ = tick.tick() => {},
                _ = self.execution.changed.notified() => {},
            }
        }
    }
}

impl ConfigurationSource {
    /// The existing root supervisor observes the actual terminal writer fault.
    /// Known-close business write failures remain retryable quota status.
    pub fn wait_quota_writer_failure(
        &self,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Arc<io::Error>> {
        Box::pin(async move {
            let storage = self
                .quota
                .current
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            let Some(owned::StorageEntry::Owned(storage)) = storage else {
                return std::future::pending().await;
            };
            loop {
                let changed = storage.execution.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let failure = storage
                    .execution
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .terminal
                    .clone();
                if let Some(failure) = failure {
                    return failure;
                }
                changed.await;
            }
        })
    }

    /// Continue a stop already requested by the actual quota registry/drop.
    /// Startup failures may have retired Engine before reaching Proxy::run;
    /// this Source still owns that original actor and its final flush book.
    pub async fn finish_quota_writer_shutdown(self: &Arc<Self>) -> io::Result<()> {
        let storage = self
            .quota
            .current
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let Some(owned::StorageEntry::Owned(storage)) = storage else {
            return Ok(());
        };
        let (configured, stopping) = {
            let state = storage
                .execution
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            (state.producer.is_some(), state.stopping)
        };
        let result = if !configured {
            storage.confirm_closed()
        } else if stopping {
            storage.shutdown_writer_async().await
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "actual quota registry has not settled its registrations",
            ))
        };
        if !storage.writer_retirement_complete() {
            self.retain_quota_cleanup();
        }
        result
    }
}
