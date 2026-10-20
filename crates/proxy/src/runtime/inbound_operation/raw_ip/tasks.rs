//! Persistent child handles for the actual raw-IP listener, outside its future.
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{Mutex as AsyncMutex, MutexGuard},
    task::{JoinError, JoinSet},
    time::Instant,
};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle, Unfinished},
    ChannelError, CloseReason, Impact, Reason, Stage,
};

pub(crate) struct RetainedTasks<T: Send + 'static> {
    inner: Arc<Inner<T>>,
}
struct Inner<T: Send + 'static> {
    label: &'static str,
    faulted: std::sync::atomic::AtomicBool,
    tasks: AsyncMutex<State<T>>,
    faults: Mutex<Vec<Arc<JoinError>>>,
}
struct State<T: Send + 'static> {
    tasks: JoinSet<T>,
    closing: bool,
}
pub(crate) struct TasksGuard<'a, T: Send + 'static>(MutexGuard<'a, State<T>>);
impl<T: Send + 'static> std::ops::Deref for TasksGuard<'_, T> {
    type Target = JoinSet<T>;
    fn deref(&self) -> &Self::Target {
        &self.0.tasks
    }
}
impl<T: Send + 'static> std::ops::DerefMut for TasksGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.tasks
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
    pub(crate) fn new(label: &'static str) -> Self {
        Self {
            inner: Arc::new(Inner {
                label,
                faulted: std::sync::atomic::AtomicBool::new(false),
                tasks: AsyncMutex::new(State {
                    tasks: JoinSet::new(),
                    closing: false,
                }),
                faults: Mutex::new(Vec::new()),
            }),
        }
    }
    /// The driver may borrow the set for its entire loop. Cancelling or
    /// unwinding that driver releases this borrow, retaining every actual handle.
    pub(crate) async fn lock(&self) -> Result<TasksGuard<'_, T>, ChannelError> {
        let guard = self.inner.tasks.lock().await;
        if guard.closing
            || self
                .inner
                .faulted
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        Ok(TasksGuard(guard))
    }
    pub(crate) fn try_lock(&self) -> Option<TasksGuard<'_, T>> {
        let guard = self.inner.tasks.try_lock().ok()?;
        (!guard.closing
            && !self
                .inner
                .faulted
                .load(std::sync::atomic::Ordering::Acquire))
        .then_some(TasksGuard(guard))
    }
    pub(crate) fn remember(&self, error: JoinError) -> Arc<JoinError> {
        let error = Arc::new(error);
        if !error.is_cancelled() {
            self.inner
                .faulted
                .store(true, std::sync::atomic::Ordering::Release);
            self.inner
                .faults
                .lock()
                .expect("raw-IP task faults poisoned")
                .push(error.clone());
        }
        error
    }
    pub(crate) fn resource(
        &self,
    ) -> Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle> {
        zero_platform_tokio::contract::lifecycle::erase_lifecycle(self.clone())
    }
    pub(crate) async fn finish(&self, tasks: &mut TasksGuard<'_, T>) {
        tasks.abort_all();
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                self.remember(error);
            }
        }
    }
    async fn close(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        let closing = async {
            let mut state = self.inner.tasks.lock().await;
            state.closing = true; // No later driver/probe may reopen this retired set.
            state.tasks.abort_all();
            while let Some(joined) = state.tasks.join_next().await {
                if let Err(error) = joined {
                    self.remember(error);
                }
                // Each consumed JoinError is retained before another suspension.
            }
        };
        let mut report = CloseReport::default();
        if tokio::time::timeout_at(deadline, closing).await.is_err() {
            report
                .unfinished
                .push(Unfinished::Resource(self.inner.label.into()));
            return Ok(report);
        }
        if let Some(source) = self
            .inner
            .faults
            .lock()
            .expect("raw-IP task faults poisoned")
            .first()
            .cloned()
        {
            return Err(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(source),
            );
        }
        report.released_resources.push(self.inner.label.into());
        Ok(report)
    }
}
impl<T: Send + 'static> ResourceLifecycle for RetainedTasks<T> {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        let _cold = self.lock().await?;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {} // Host stops the owning driver first.
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.close(deadline).await
    }
}

#[cfg(test)]
#[path = "tasks/tests.rs"]
mod tests;
