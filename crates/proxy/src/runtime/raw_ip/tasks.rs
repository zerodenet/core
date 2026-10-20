//! Bounded, fenced child handles retained outside the actual listener future.
mod output;
pub(crate) use output::TaskOutput;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{Mutex as AsyncMutex, MutexGuard},
    task::{JoinError, JoinSet},
    time::Instant,
};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle, Unfinished},
    ChannelError, CloseReason, Impact, Reason, Stage,
};
pub(crate) type Source = Arc<dyn std::error::Error + Send + Sync>;

pub(crate) struct RetainedTasks<T: Send + 'static> {
    inner: Arc<Inner<T>>,
}
struct Inner<T: Send + 'static> {
    label: &'static str,
    limit: usize,
    tasks: AsyncMutex<JoinSet<T>>,
    admission: Mutex<Admission>,
}
#[derive(Default)]
struct Admission {
    revoked: bool,
    drained: bool,
    first: Option<ActualTaskFailure>,
    additional: usize,
}
#[derive(Clone, Debug)]
struct ActualTaskFailure {
    task: tokio::task::Id,
    source: Source,
}
#[derive(Debug)]
struct RetainedFailures {
    first: ActualTaskFailure,
    additional: usize,
}
impl std::fmt::Display for RetainedFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "actual raw-IP child {} failed ({} additional joined failures): {}",
            self.first.task, self.additional, self.first.source
        )
    }
}
impl std::error::Error for RetainedFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.first.source.as_ref())
    }
}
/// No DerefMut: every registration, including one through a long-lived guard,
/// must pass the same synchronous admission fence as revoke and first failure.
pub(crate) struct TasksGuard<'a, T: Send + 'static> {
    tasks: MutexGuard<'a, JoinSet<T>>,
    owner: &'a Inner<T>,
}
impl<T: Send + 'static> TasksGuard<'_, T> {
    pub(crate) fn spawn(
        &mut self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Result<tokio::task::Id, ChannelError> {
        let admission = self
            .owner
            .admission
            .lock()
            .expect("raw-IP task admission poisoned");
        if admission.revoked {
            return Err(rejected(Reason::Revoked));
        }
        if self.tasks.len() >= self.owner.limit {
            return Err(rejected(Reason::Capacity));
        }
        Ok(self.tasks.spawn(future).id())
    }
    pub(crate) fn len(&self) -> usize {
        self.tasks.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
    pub(crate) fn abort_all(&mut self) {
        self.tasks.abort_all();
    }
    pub(crate) async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.tasks.join_next().await
    }
    pub(crate) fn try_join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.tasks.try_join_next()
    }
}
impl<T: Send + 'static> Clone for RetainedTasks<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<T: Send + 'static> RetainedTasks<T> {
    pub(crate) fn new(label: &'static str, limit: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                label,
                limit,
                tasks: AsyncMutex::new(JoinSet::new()),
                admission: Mutex::new(Admission::default()),
            }),
        }
    }
    fn admitted(&self) -> bool {
        !self
            .inner
            .admission
            .lock()
            .expect("raw-IP task admission poisoned")
            .revoked
    }
    pub(crate) async fn lock(&self) -> Result<TasksGuard<'_, T>, ChannelError> {
        let tasks = self.inner.tasks.lock().await;
        if !self.admitted() {
            return Err(rejected(Reason::Revoked));
        }
        Ok(TasksGuard {
            tasks,
            owner: &self.inner,
        })
    }
    pub(crate) fn try_lock(&self) -> Option<TasksGuard<'_, T>> {
        let tasks = self.inner.tasks.try_lock().ok()?;
        self.admitted().then_some(TasksGuard {
            tasks,
            owner: &self.inner,
        })
    }
    fn revoke(&self) {
        self.inner
            .admission
            .lock()
            .expect("raw-IP task admission poisoned")
            .revoked = true;
    }
    /// Synchronous cancellation is not a join receipt. The same owner must
    /// subsequently await shutdown even when a guard is temporarily borrowed.
    /// Set only after consuming all original handles with admission revoked.
    /// Abort, body completion and an empty-at-construction set never set it.
    pub(crate) fn is_drained(&self) -> bool {
        self.inner
            .admission
            .lock()
            .expect("raw-IP task admission poisoned")
            .drained
    }
    pub(crate) fn request_abort(&self) {
        self.revoke();
        if let Ok(mut tasks) = self.inner.tasks.try_lock() {
            tasks.abort_all();
        }
    }
    fn fault(&self, task: tokio::task::Id, source: Source) {
        let mut admission = self
            .inner
            .admission
            .lock()
            .expect("raw-IP task admission poisoned");
        admission.revoked = true;
        if admission.first.is_none() {
            admission.first = Some(ActualTaskFailure { task, source });
        } else {
            admission.additional = admission.additional.saturating_add(1);
        }
    }
    pub(crate) fn remember(&self, error: JoinError) -> Arc<JoinError> {
        let source = Arc::new(error);
        if !source.is_cancelled() {
            self.fault(source.id(), source.clone());
        }
        source
    }
}
impl<T: TaskOutput> RetainedTasks<T> {
    /// Reclaim only actual joined handles before another bounded registration.
    /// Normalized original failures close the same admission fence as shutdown.
    pub(crate) fn reap_ready(&self, guard: &mut TasksGuard<'_, T>) {
        assert!(std::ptr::eq(guard.owner, self.inner.as_ref()));
        while let Some(result) = guard.tasks.try_join_next_with_id() {
            self.joined(result);
        }
    }
    /// Let an existing I/O driver supervise one original child without a
    /// detached monitor. Cancellation leaves its handle in this same JoinSet.
    pub(crate) async fn wait_one(&self) -> Result<bool, ChannelError> {
        let mut tasks = self.inner.tasks.lock().await;
        let result = tasks.join_next_with_id().await;
        let joined = result.is_some();
        if let Some(result) = result {
            self.joined(result);
        }
        // Only the original failure is exposed; this is not a resource/close
        // receipt while other children may still occupy the set.
        if let Some(error) = self.fault_error() {
            Err(error)
        } else {
            Ok(joined)
        }
    }
    pub(crate) fn resource(
        &self,
    ) -> Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle> {
        zero_platform_tokio::contract::lifecycle::erase_lifecycle(self.clone())
    }
    fn joined(&self, result: Result<(tokio::task::Id, T), JoinError>) {
        match result {
            Ok((id, output)) => {
                if let Some(source) = output.into_failure() {
                    self.fault(id, source);
                }
            }
            Err(error) => {
                self.remember(error);
            }
        }
    }
    pub(crate) async fn finish(&self, guard: &mut TasksGuard<'_, T>) {
        assert!(
            std::ptr::eq(guard.owner, self.inner.as_ref()),
            "same original child set"
        );
        self.revoke();
        guard.tasks.abort_all();
        while let Some(joined) = guard.tasks.join_next_with_id().await {
            self.joined(joined);
        }
        self.inner
            .admission
            .lock()
            .expect("raw-IP task admission poisoned")
            .drained = true;
    }
    async fn join_all(&self) {
        self.revoke();
        let mut tasks = self.inner.tasks.lock().await;
        tasks.abort_all();
        while let Some(joined) = tasks.join_next_with_id().await {
            self.joined(joined);
        }
        self.inner
            .admission
            .lock()
            .expect("raw-IP task admission poisoned")
            .drained = true;
    }
    fn fault_error(&self) -> Option<ChannelError> {
        let admission = self
            .inner
            .admission
            .lock()
            .expect("raw-IP task admission poisoned");
        if let Some(first) = &admission.first {
            return Some(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(
                    RetainedFailures {
                        first: first.clone(),
                        additional: admission.additional,
                    },
                ),
            );
        }
        None
    }
    fn joined_report(&self) -> Result<CloseReport, ChannelError> {
        if let Some(error) = self.fault_error() {
            return Err(error);
        }
        let mut report = CloseReport::default();
        report.released_resources.push(self.inner.label.into());
        Ok(report)
    }
    /// Compatibility for existing explicit waits with no caller deadline.
    /// This still consumes the actual handles and reports their original fault.
    pub(crate) async fn join_unbounded(&self) -> Result<CloseReport, ChannelError> {
        self.join_all().await;
        self.joined_report()
    }
    pub(crate) async fn close(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(); // Fences even an already-held guard before the first await.
        let mut report = CloseReport::default();
        if tokio::time::timeout_at(deadline, self.join_all())
            .await
            .is_err()
        {
            report
                .unfinished
                .push(Unfinished::Resource(self.inner.label.into()));
            return Ok(report);
        }
        self.joined_report()
    }
}
impl<T: TaskOutput> ResourceLifecycle for RetainedTasks<T> {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        self.admitted()
            .then_some(())
            .ok_or_else(|| rejected(Reason::Revoked))
    }
    fn revoke(&self, _: CloseReason) {
        RetainedTasks::revoke(self);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.close(deadline).await
    }
}
fn rejected(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
}
#[cfg(test)]
mod tests;
