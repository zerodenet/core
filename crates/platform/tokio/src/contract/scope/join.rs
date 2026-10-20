//! Optional observation of actual JoinSet consumption. No witness owns a task,
//! supervisor, runtime, I/O, or permission to declare a resource closed.
use super::*;
use std::sync::Weak;

pub(super) struct Registration {
    pub(super) id: TaskId,
    class: TaskClass,
    witness: Option<Weak<JoinedState>>,
}
#[derive(Debug, Default)]
struct JoinedState {
    joined: Mutex<Option<TaskJoined>>,
    changed: Notify,
}
/// A cancel-safe observer, completed only by the scope's actual join consumer.
/// Dropping the scope, the task future, or a wait future does not complete it.
#[derive(Clone, Debug)]
pub struct TaskJoinWitness(Arc<JoinedState>);
impl TaskJoinWitness {
    fn new() -> Self {
        Self(Arc::default())
    }
    /// Read-only observation; this neither polls JoinSet nor frees scope budget.
    pub fn is_joined(&self) -> bool {
        self.0
            .joined
            .lock()
            .expect("task join witness poisoned")
            .is_some()
    }
    pub fn try_joined(&self) -> Option<TaskJoined> {
        self.0
            .joined
            .lock()
            .expect("task join witness poisoned")
            .clone()
    }
    /// Wait for the supervisor to consume this exact task's native exit.
    /// Cancelling this wait retains the observation for a later retry. If the
    /// owner is dropped without joining, this witness never declares success.
    pub async fn wait(&self) -> TaskJoined {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(joined) = self.try_joined() {
                return joined;
            }
            changed.await;
        }
    }
}
/// Opaque evidence of one real consumed task exit. It is never an I/O-close
/// receipt and has no public constructor or mutable identity/result fields.
#[derive(Clone, Debug)]
pub struct TaskJoined(Arc<Joined>);
#[derive(Debug)]
struct Joined {
    id: TaskId,
    class: TaskClass,
    result: Result<(), Arc<ComponentError>>,
}
impl TaskJoined {
    pub fn id(&self) -> TaskId {
        self.0.id
    }
    pub fn class(&self) -> TaskClass {
        self.0.class
    }
    /// Every observer receives the very same original task-error Arc.
    pub fn result(&self) -> Result<(), Arc<ComponentError>> {
        self.0.result.clone()
    }
    pub(super) fn same_receipt(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
#[derive(Debug)]
struct SharedFailure(Arc<ComponentError>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
/// Only tracked exits with a live witness need this sharing layer. Preserve
/// every decision field; source() explicitly reaches that same original Arc.
fn forward(source: Arc<ComponentError>) -> ComponentError {
    let mut error = ChannelError::new(source.stage, source.reason, source.impact);
    error.component = source.component;
    error.session = source.session;
    error.delivery = source.delivery;
    error.retry = source.retry;
    error.with_source(SharedFailure(source))
}
/// The sole constructor/completion site, called only from State::exit after a
/// JoinSet consumer returns the real native exit and removes its registration.
pub(super) fn complete(
    record: Registration,
    result: Result<(), ComponentError>,
    hosted: bool,
) -> CompletedExit {
    let witness = record.witness.and_then(|weak| weak.upgrade());
    if !hosted && witness.is_none() {
        return CompletedExit::Ordinary(TaskExit {
            id: record.id,
            class: record.class,
            result,
        });
    }
    let result = result.map_err(Arc::new);
    let receipt = TaskJoined(Arc::new(Joined {
        id: record.id,
        class: record.class,
        result: result.clone(),
    }));
    if let Some(witness) = witness {
        let mut joined = witness.joined.lock().expect("task join witness poisoned");
        assert!(joined.is_none(), "a native task exit is consumed once");
        *joined = Some(receipt.clone());
        drop(joined);
        witness.changed.notify_waiters();
    }
    if hosted {
        return CompletedExit::Hosted(hosted::Completion::new(receipt));
    }
    CompletedExit::Ordinary(TaskExit {
        id: record.id,
        class: record.class,
        result: result.map_err(forward),
    })
}

impl TaskRegistrar {
    pub fn cancellation(&self) -> CancellationToken {
        self.0.cancel.child_token()
    }
    pub fn spawn(
        &self,
        class: TaskClass,
        task: impl Future<Output = Result<(), ComponentError>> + Send + 'static,
    ) -> Result<TaskId, SpawnError> {
        self.register(class, task, None)
    }
    /// Same registration, budget, owner and supervision as spawn. The optional
    /// witness observes actual joins; it cannot perform one or release capacity.
    pub fn spawn_tracked(
        &self,
        class: TaskClass,
        task: impl Future<Output = Result<(), ComponentError>> + Send + 'static,
    ) -> Result<(TaskId, TaskJoinWitness), SpawnError> {
        let witness = TaskJoinWitness::new();
        let id = self.register(class, task, Some(Arc::downgrade(&witness.0)))?;
        Ok((id, witness))
    }
    fn register(
        &self,
        class: TaskClass,
        task: impl Future<Output = Result<(), ComponentError>> + Send + 'static,
        witness: Option<Weak<JoinedState>>,
    ) -> Result<TaskId, SpawnError> {
        let mut state = self.0.state.lock().expect("scope state poisoned");
        if state.closing {
            return Err(SpawnError::Closing);
        }
        if !state.registration_open {
            return Err(SpawnError::NotStarted);
        }
        state.reap();
        // As before, undelivered exits consume the declared budget. A witness
        // observing a join cannot acknowledge the supervisor's exit event.
        if state.records.len() + state.completed.len() >= state.limit {
            drop(state);
            self.0.notify_hosted();
            return Err(SpawnError::Capacity);
        }
        let id = allocate_task_id()?;
        let handle = state.tasks.spawn_on(task, &self.0.runtime);
        state
            .records
            .insert(handle.id(), Registration { id, class, witness });
        drop(state);
        self.0.notify_hosted();
        Ok(id)
    }
}

#[cfg(test)]
mod tests;
