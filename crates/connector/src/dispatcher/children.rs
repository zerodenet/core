//! Original child handles remain in the same dispatcher owner even if its loop panics.
use super::*;
use std::sync::atomic::AtomicBool;

pub(crate) struct AsyncChild {
    state: tokio::sync::Mutex<AsyncState>,
    abort: tokio::task::AbortHandle,
    cancellation_requested: AtomicBool,
    failure_recorded: AtomicBool,
}
struct AsyncState {
    task: tokio::task::JoinHandle<()>,
    exit: Option<Result<(), Arc<tokio::task::JoinError>>>,
}
struct SourceChild {
    task: Option<std::thread::JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    budget: Arc<source::SourceBudget>,
}
#[derive(Default)]
pub(crate) struct DispatcherChildren {
    asynchronous: Mutex<Vec<Arc<AsyncChild>>>,
    source: Mutex<Option<SourceChild>>,
    failures: Mutex<Vec<Arc<tokio::task::JoinError>>>,
    source_failure: Mutex<Option<Box<dyn std::any::Any + Send>>>,
}
impl DispatcherChildren {
    pub(crate) fn register(&self, task: tokio::task::JoinHandle<()>) -> Arc<AsyncChild> {
        let child = Arc::new(AsyncChild {
            abort: task.abort_handle(),
            state: tokio::sync::Mutex::new(AsyncState { task, exit: None }),
            cancellation_requested: AtomicBool::new(false),
            failure_recorded: AtomicBool::new(false),
        });
        self.asynchronous.lock().unwrap().push(child.clone());
        child
    }
    pub(crate) async fn join(
        &self,
        child: &Arc<AsyncChild>,
    ) -> Result<(), Arc<tokio::task::JoinError>> {
        let mut state = child.state.lock().await;
        if state.exit.is_none() {
            state.exit = Some((&mut state.task).await.map_err(Arc::new));
        }
        let result = state.exit.as_ref().unwrap().clone();
        if let Err(error) = &result {
            if error.is_cancelled() && child.cancellation_requested.load(Ordering::Acquire) {
                return Ok(());
            }
            if !child.failure_recorded.swap(true, Ordering::AcqRel) {
                self.failures.lock().unwrap().push(error.clone());
            }
        }
        result
    }
    pub(crate) fn abort(&self, child: &Arc<AsyncChild>) {
        child.cancellation_requested.store(true, Ordering::Release);
        child.abort.abort(); // request only, original JoinHandle remains installed
    }
    pub(super) fn source(
        &self,
        task: std::thread::JoinHandle<()>,
        cancelled: Arc<AtomicBool>,
        budget: Arc<source::SourceBudget>,
    ) {
        *self.source.lock().unwrap() = Some(SourceChild {
            task: Some(task),
            cancelled,
            budget,
        });
    }
    pub(super) fn join_source(&self) {
        if let Some(source) = self.source.lock().unwrap().as_mut() {
            source.cancelled.store(true, Ordering::Release);
            source.budget.close();
            if let Some(task) = source.task.take() {
                if let Err(error) = task.join() {
                    *self.source_failure.lock().unwrap() = Some(error);
                }
            }
        }
    }
    pub(crate) async fn join_all(&self) {
        if let Some(source) = self.source.lock().unwrap().as_ref() {
            source.cancelled.store(true, Ordering::Release);
            source.budget.close();
        }
        let children = self.asynchronous.lock().unwrap().clone();
        for child in children {
            let _ = self.join(&child).await;
        }
        self.join_source();
    }
    /// Consumed real joins only; task.is_finished and stop notifications do not qualify.
    pub(crate) fn retirement_complete(&self) -> bool {
        let children = self.asynchronous.lock().unwrap().clone();
        if children.iter().any(|child| {
            child
                .state
                .try_lock()
                .map_or(true, |state| state.exit.is_none())
        }) {
            return false;
        }
        self.source
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|source| source.task.is_none())
    }
    pub(super) fn first_failure(&self) -> Option<Arc<tokio::task::JoinError>> {
        self.failures.lock().unwrap().first().cloned()
    }
    pub(crate) fn failure_count(&self) -> usize {
        self.failures.lock().unwrap().len()
            + usize::from(self.source_failure.lock().unwrap().is_some())
    }
    pub(crate) async fn blocking_publish(
        &self,
        publish: impl FnOnce() -> zero_api::ApiResult<zero_api::PublishResult> + Send + 'static,
    ) -> zero_api::ApiResult<zero_api::PublishResult> {
        if self.failure_count() != 0 {
            return Err(zero_api::ApiError::new(
                zero_api::ApiErrorCode::Internal,
                "original dispatcher child failure blocks new blocking publishers",
            ));
        }
        let (send, receive) = tokio::sync::oneshot::channel();
        let child = self.register(tokio::task::spawn_blocking(move || {
            let _ = send.send(publish());
        }));
        self.join(&child).await.map_err(|error| {
            zero_api::ApiError::new(
                zero_api::ApiErrorCode::Internal,
                format!("original blocking publisher failed: {error}"),
            )
        })?;
        let result = receive.await.map_err(|_| {
            zero_api::ApiError::new(
                zero_api::ApiErrorCode::Internal,
                "original blocking publisher lost its result",
            )
        })?;
        // Successful original join permits this finite slot to be retired; no
        // per-delivery task history accumulates in the actual dispatcher owner.
        self.asynchronous
            .lock()
            .unwrap()
            .retain(|original| !Arc::ptr_eq(original, &child));
        result
    }
}

#[cfg(all(test, feature = "sink-jsonl"))]
mod tests;
