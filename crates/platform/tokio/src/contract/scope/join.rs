//! Optional observation of actual JoinSet consumption. No witness owns a task,
//! supervisor, runtime, I/O, or permission to declare a resource closed.
use super::*;
use std::sync::Weak;
mod acceptance;

pub(super) struct Registration {
    pub(super) id: TaskId,
    class: TaskClass,
    witness: Option<Weak<JoinedState>>,
    global: Option<budget::TaskRegistrationCredit>,
}
pub(super) struct JoinedState {
    task: std::sync::OnceLock<TaskId>,
    native_task: std::sync::OnceLock<tokio::task::Id>,
    hosted: bool,
    owner: Weak<Shared>,
    joined: Mutex<Option<TaskJoined>>,
    changed: Notify,
}
pub(super) enum Notification {
    Joined(Arc<JoinedState>),
    Released(TaskJoined),
}
impl Notification {
    pub(super) fn released(joined: TaskJoined) -> Self {
        // Only actual completed-entry removal reaches this constructor.
        joined
            .0
            .registration_released
            .store(true, Ordering::Release);
        Self::Released(joined)
    }
    pub(super) fn notify(&self) {
        match self {
            Self::Joined(witness) => witness.changed.notify_waiters(),
            Self::Released(joined) => joined.0.registration_changed.notify_waiters(),
        }
    }
}
impl std::fmt::Debug for JoinedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinedState")
            .field("hosted", &self.hosted)
            .field("joined", &self.joined)
            .finish_non_exhaustive()
    }
}
/// A cancel-safe observer, completed only by the scope's actual join consumer.
/// Dropping the scope, the task future, or a wait future does not complete it.
#[derive(Clone, Debug)]
pub struct TaskJoinWitness(Arc<JoinedState>);
impl TaskJoinWitness {
    /// Original Tokio registration identity, never a native IO close receipt.
    pub fn native_task_id(&self) -> Option<tokio::task::Id> {
        self.0.native_task.get().copied()
    }
    /// Original Scope registration identity, published before spawn returns.
    pub fn task_id(&self) -> Option<TaskId> {
        self.0.task.get().copied()
    }
    /// Weak identity of this actual registration's original scope. This does
    /// not join, acknowledge, retain the scope or grant another registration.
    pub fn scope_identity(&self) -> super::TaskScopeIdentity {
        super::TaskScopeIdentity::from_owner(self.0.owner.clone())
    }
    /// Match this actual consumed native exit, including its original result
    /// Arc. Equal task numbers or another scope's exit cannot substitute it.
    pub fn matches_joined(&self, joined: &TaskJoined) -> bool {
        self.try_joined()
            .is_some_and(|actual| actual.same_receipt(joined))
    }
    /// Compare the original registration observer, without joining, waking,
    /// retaining the scope, or granting permission to acknowledge an exit.
    pub fn same_registration(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    fn new(owner: &Arc<Shared>) -> Self {
        let hosted = owner.lock_state().hosted;
        Self(Arc::new(JoinedState {
            hosted,
            task: std::sync::OnceLock::new(),
            native_task: std::sync::OnceLock::new(),
            owner: Arc::downgrade(owner),
            joined: Mutex::new(None),
            changed: Notify::new(),
        }))
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
    /// Observe the same registration's actual budget return. A consumed join
    /// alone is insufficient for HostedScope: its host must really ACK and
    /// remove this completed entry. Never ACKs, polls joins or closes native IO.
    pub async fn wait_registration_released(&self) -> TaskJoined {
        let joined = self.wait().await;
        loop {
            let changed = joined.0.registration_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if joined.registration_released() {
                return joined.clone();
            }
            changed.await;
        }
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
    failure_accepted: std::sync::atomic::AtomicBool,
    registration_released: std::sync::atomic::AtomicBool,
    registration_changed: Notify,
}
impl TaskJoined {
    pub fn registration_released(&self) -> bool {
        self.0.registration_released.load(Ordering::Acquire)
    }

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
    /// The original local receiver accepted this exact failure. This is not
    /// Host registration ACK, original IO closure, or whole-Scope retirement.
    pub fn failure_accepted(&self) -> bool {
        self.0.failure_accepted.load(Ordering::Acquire)
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
    if let Some(receipt) = source.cleanup_failure_receipt() {
        error = error.with_cleanup_failure_receipt(receipt.clone());
    }
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
        return CompletedExit::Ordinary(
            TaskExit {
                id: record.id,
                class: record.class,
                result,
            },
            None,
            None,
            record.global,
        );
    }
    let result = result.map_err(Arc::new);
    let receipt = TaskJoined(Arc::new(Joined {
        id: record.id,
        class: record.class,
        result: result.clone(),
        failure_accepted: std::sync::atomic::AtomicBool::new(false),
        registration_released: std::sync::atomic::AtomicBool::new(false),
        registration_changed: Notify::new(),
    }));
    let notification = if let Some(witness) = witness {
        let mut joined = witness.joined.lock().expect("task join witness poisoned");
        assert!(joined.is_none(), "a native task exit is consumed once");
        *joined = Some(receipt.clone());
        drop(joined);
        Some(Notification::Joined(witness))
    } else {
        None
    };
    if hosted {
        return CompletedExit::Hosted(
            hosted::Completion::new(receipt, record.global),
            notification,
        );
    }
    CompletedExit::Ordinary(
        TaskExit {
            id: record.id,
            class: record.class,
            result: result.map_err(forward),
        },
        notification,
        Some(receipt),
        record.global,
    )
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
        self.register_tracked_with(class, move |tasks, runtime| tasks.spawn_on(task, runtime))
    }
    pub(super) fn register_tracked_with(
        &self,
        class: TaskClass,
        spawn: impl FnOnce(
            &mut JoinSet<Result<(), ComponentError>>,
            &tokio::runtime::Handle,
        ) -> tokio::task::AbortHandle,
    ) -> Result<(TaskId, TaskJoinWitness), SpawnError> {
        let witness = TaskJoinWitness::new(&self.0);
        let id = self.register_with(class, Some(Arc::downgrade(&witness.0)), spawn)?;
        let _ = witness.0.task.set(id);
        Ok((id, witness))
    }
    fn register(
        &self,
        class: TaskClass,
        task: impl Future<Output = Result<(), ComponentError>> + Send + 'static,
        witness: Option<Weak<JoinedState>>,
    ) -> Result<TaskId, SpawnError> {
        self.register_with(class, witness, move |tasks, runtime| {
            tasks.spawn_on(task, runtime)
        })
    }
    fn register_with(
        &self,
        class: TaskClass,
        witness: Option<Weak<JoinedState>>,
        spawn: impl FnOnce(
            &mut JoinSet<Result<(), ComponentError>>,
            &tokio::runtime::Handle,
        ) -> tokio::task::AbortHandle,
    ) -> Result<TaskId, SpawnError> {
        self.register_with_launch(class, witness, spawn, None)
    }
    pub(super) fn register_reserved(
        &self,
        class: TaskClass,
        reserved: &mut TaskLaunch,
        spawn: impl FnOnce(
            &mut JoinSet<Result<(), ComponentError>>,
            &tokio::runtime::Handle,
        ) -> tokio::task::AbortHandle,
    ) -> Result<(TaskId, TaskJoinWitness), SpawnError> {
        let witness = TaskJoinWitness::new(&self.0);
        let id = self.register_with_launch(
            class,
            Some(Arc::downgrade(&witness.0)),
            spawn,
            Some(reserved),
        )?;
        let _ = witness.0.task.set(id);
        Ok((id, witness))
    }
    fn register_with_launch(
        &self,
        class: TaskClass,
        witness: Option<Weak<JoinedState>>,
        spawn: impl FnOnce(
            &mut JoinSet<Result<(), ComponentError>>,
            &tokio::runtime::Handle,
        ) -> tokio::task::AbortHandle,
        reserved: Option<&mut TaskLaunch>,
    ) -> Result<TaskId, SpawnError> {
        if self.1 {
            return Err(SpawnError::Capacity);
        }
        let mut state = self.0.lock_state();
        if state.closing {
            return Err(SpawnError::Closing);
        }
        if !state.registration_open {
            return Err(SpawnError::NotStarted);
        }
        state.reap();
        // As before, undelivered exits consume the declared budget. A witness
        // observing a join cannot acknowledge the supervisor's exit event.
        if reserved.is_none()
            && state.records.len() + state.completed.len() + state.pending_launches >= state.limit
        {
            drop(state);
            self.0.notify_hosted();
            return Err(SpawnError::Capacity);
        }
        let id = allocate_task_id()?;
        let mut global = match reserved {
            Some(reserved) => reserved.consume(&self.0, &mut state),
            None => self
                .0
                .budget
                .get()
                .map(|budget| budget.reserve())
                .transpose()?,
        };
        if let Some(global) = &mut global {
            global.mark_spawned();
        }
        state.issued_tasks = true;
        let handle = spawn(&mut state.tasks, &self.0.runtime);
        if let Some(witness) = witness.as_ref().and_then(Weak::upgrade) {
            let _ = witness.native_task.set(handle.id());
        }
        state.records.insert(
            handle.id(),
            Registration {
                id,
                class,
                witness,
                global,
            },
        );
        drop(state);
        self.0.notify_hosted();
        Ok(id)
    }
}

#[cfg(test)]
mod tests;

mod error;
pub use error::TaskJoinError;
