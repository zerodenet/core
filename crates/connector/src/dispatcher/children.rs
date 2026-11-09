//! Original child handles remain in the same dispatcher owner even if its loop panics.
use super::*;
use crate::capacity::{DispatcherCapacity, TaskCharge};
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
    charge: Option<TaskCharge>,
}
struct SourceChild {
    task: Option<std::thread::JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    budget: Arc<source::SourceBudget>,
    charge: Option<TaskCharge>,
}
#[derive(Default)]
pub(crate) struct DispatcherChildren {
    capacity: Arc<DispatcherCapacity>,
    source_pending: Mutex<Option<TaskCharge>>,
    asynchronous: Mutex<Vec<Arc<AsyncChild>>>,
    source: Mutex<Option<SourceChild>>,
    failures: Mutex<Vec<Arc<tokio::task::JoinError>>>,
    source_failure: Mutex<Option<Box<dyn std::any::Any + Send>>>,
}
impl DispatcherChildren {
    pub(crate) fn with_capacity(capacity: Arc<DispatcherCapacity>) -> Self {
        Self {
            capacity,
            source_pending: Mutex::new(None),
            asynchronous: Mutex::new(Vec::new()),
            source: Mutex::new(None),
            failures: Mutex::new(Vec::new()),
            source_failure: Mutex::new(None),
        }
    }
    pub(crate) fn task_charge(&self) -> std::io::Result<TaskCharge> {
        self.capacity.task()
    }
    pub(crate) fn register(
        &self,
        task: tokio::task::JoinHandle<()>,
        charge: TaskCharge,
    ) -> Arc<AsyncChild> {
        let child = Arc::new(AsyncChild {
            abort: task.abort_handle(),
            state: tokio::sync::Mutex::new(AsyncState {
                task,
                exit: None,
                charge: Some(charge),
            }),
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
        let cancelled = result.as_ref().is_err_and(|error| {
            error.is_cancelled() && child.cancellation_requested.load(Ordering::Acquire)
        });
        if let Err(error) = &result {
            if !cancelled {
                if !child.failure_recorded.swap(true, Ordering::AcqRel) {
                    self.failures.lock().unwrap().push(error.clone());
                }
            }
        }
        // Retain the exact exit/fault first, then refund this actual job only
        // after releasing the child state lock. Body completion/abort cannot.
        let charge = if result.is_ok() || cancelled {
            state.charge.take()
        } else {
            None
        };
        // Failed originals keep their admission in this same retained receiver.
        drop(state);
        if let Some(charge) = charge {
            charge.complete();
        }
        if cancelled {
            Ok(())
        } else {
            result
        }
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
            charge: self.source_pending.lock().unwrap().take(),
        });
    }
    pub(super) fn reserve_source(&self) -> std::io::Result<()> {
        *self.source_pending.lock().unwrap() = Some(self.capacity.task()?);
        Ok(())
    }
    /// Called only on the original std::thread::Builder::spawn Err branch.
    pub(super) fn source_not_started(&self) {
        let charge = self.source_pending.lock().unwrap().take();
        if let Some(charge) = charge {
            charge.complete();
        }
    }
    pub(super) fn join_source(&self) {
        let mut released = None;
        if let Some(source) = self.source.lock().unwrap().as_mut() {
            source.cancelled.store(true, Ordering::Release);
            source.budget.close();
            if let Some(task) = source.task.take() {
                match task.join() {
                    Ok(()) => released = source.charge.take(),
                    Err(error) => *self.source_failure.lock().unwrap() = Some(error),
                }
            }
        }
        if let Some(charge) = released {
            charge.complete();
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
        if self.source_pending.lock().unwrap().is_some() {
            return false;
        }
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
        // Backpressure this existing sink receiver rather than creating an
        // uncharged publisher or discarding an accepted delivery. Cold
        // validation guarantees at least one publisher can make progress.
        let charge = self.capacity.publisher().await.map_err(|error| {
            zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, error.to_string())
        })?;
        if self.failure_count() != 0 {
            charge.complete(); // No spawn was attempted after this admission.
            return Err(zero_api::ApiError::new(
                zero_api::ApiErrorCode::Internal,
                "original dispatcher child failed while publisher waited for admission",
            ));
        }
        let (start, started) = std::sync::mpsc::sync_channel(1);
        let child = self.register(
            tokio::task::spawn_blocking(move || {
                if started.recv().is_err() {
                    return;
                }
                let _ = send.send(publish());
            }),
            charge,
        );
        let _ = start.send(());
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
