//! A bounded task owner using an existing runtime. No actor or runtime per scope.
use std::{
    collections::{HashMap, VecDeque},
    future::{poll_fn, Future},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};
use tokio::{
    runtime::Handle,
    sync::Notify,
    task::{Id, JoinError, JoinSet},
    time::{timeout_at, Instant},
};
use tokio_util::sync::CancellationToken;
use zero_core::component::TaskId;

use super::{
    lifecycle::{CloseReport, ComponentError, TaskFailure, Unfinished},
    ChannelError, Impact, Reason, Stage,
};

pub use super::lifecycle::TaskClass;
mod join;
pub use join::{TaskJoinWitness, TaskJoined};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnError {
    NotStarted,
    Capacity,
    Closing,
    IdentityExhausted,
}
impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotStarted => "component build cannot register tasks before host start",
            Self::Capacity => "task scope capacity exhausted",
            Self::Closing => "task scope is closing",
            Self::IdentityExhausted => "task identity space exhausted",
        })
    }
}
impl std::error::Error for SpawnError {}

#[derive(Debug)]
pub struct TaskExit {
    pub id: TaskId,
    pub class: TaskClass,
    pub result: Result<(), ComponentError>,
}

struct State {
    registration_open: bool,
    closing: bool,
    limit: usize,
    tasks: JoinSet<Result<(), ComponentError>>,
    records: HashMap<Id, join::Registration>,
    completed: VecDeque<TaskExit>,
    closing_report: Option<Box<CloseReport>>,
}

struct Shared {
    state: Mutex<State>,
    changed: Notify,
    cancel: CancellationToken,
    runtime: Handle,
}

/// Cloneable registration authority. It cannot declare resources closed.
#[derive(Clone)]
pub struct TaskRegistrar(Arc<Shared>);
pub struct TaskScope(Arc<Shared>);

// Reports can combine component, handshake and connection scopes.
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(0);

fn allocate_task_id() -> Result<TaskId, SpawnError> {
    let mut current = NEXT_TASK_ID.load(Ordering::Relaxed);
    loop {
        let next = current
            .checked_add(1)
            .ok_or(SpawnError::IdentityExhausted)?;
        match NEXT_TASK_ID.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(TaskId(next)),
            Err(observed) => current = observed,
        }
    }
}

impl State {
    fn exit(&mut self, result: Result<(Id, Result<(), ComponentError>), JoinError>) -> TaskExit {
        let native_id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        // JoinSet and records are modified together under this same short lock.
        let record = self
            .records
            .remove(&native_id)
            .expect("registered scope task");
        let result = match result {
            Ok((_, result)) => result,
            Err(source) => {
                Err(
                    ChannelError::new(Stage::Close, Reason::Internal, Impact::Component)
                        .with_source(source),
                )
            }
        };
        join::complete(record, result)
    }

    fn reap(&mut self) {
        while let Some(result) = self.tasks.try_join_next_with_id() {
            let exit = self.exit(result);
            self.completed.push_back(exit);
        }
    }

    fn record_closed(&mut self, exit: TaskExit) {
        let report = self.closing_report.get_or_insert_with(Box::default);
        if let Err(error) = exit.result {
            report.failed_tasks.push(TaskFailure {
                id: exit.id,
                class: exit.class,
                error,
            });
        }
        report.exited_tasks.push(exit.id);
    }

    fn take_close_report(&mut self) -> CloseReport {
        let mut report = self
            .closing_report
            .take()
            .map(|report| *report)
            .unwrap_or_default();
        report.unfinished.extend(
            self.records
                .values()
                .map(|record| Unfinished::Task(record.id)),
        );
        report
    }
}

impl TaskScope {
    pub fn new(runtime: Handle, limit: NonZeroUsize) -> Self {
        Self::with_limit(runtime, limit.get())
    }

    /// A real owner for a component declaring no internal tasks. Any attempted
    /// registration fails with Capacity rather than inventing a default budget.
    pub fn empty(runtime: Handle) -> Self {
        Self::with_limit(runtime, 0)
    }

    /// The host retains the sole scope owner while Factory::build receives only
    /// a registrar. Registration opens explicitly after that real owner is
    /// installed and the host enters lifecycle start. Ordinary new/empty scopes
    /// keep their existing immediate-registration semantics.
    pub fn suspended(runtime: Handle, limit: Option<NonZeroUsize>) -> Self {
        let scope = Self::with_limit(runtime, limit.map_or(0, NonZeroUsize::get));
        scope
            .0
            .state
            .lock()
            .expect("scope state poisoned")
            .registration_open = false;
        scope
    }

    /// Only the non-cloneable scope owner can grant registration. A retained
    /// protocol registrar cannot open build access or reopen a stopping scope.
    pub fn enable_registration(&mut self) -> Result<(), SpawnError> {
        let mut state = self.0.state.lock().expect("scope state poisoned");
        if state.closing {
            return Err(SpawnError::Closing);
        }
        state.registration_open = true;
        Ok(())
    }

    fn with_limit(runtime: Handle, limit: usize) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                registration_open: true,
                closing: false,
                limit,
                tasks: JoinSet::new(),
                records: HashMap::new(),
                completed: VecDeque::new(),
                closing_report: None,
            }),
            changed: Notify::new(),
            cancel: CancellationToken::new(),
            runtime,
        }))
    }

    pub fn registrar(&self) -> TaskRegistrar {
        TaskRegistrar(self.0.clone())
    }

    /// The supervisor lives outside the scope it supervises.
    pub async fn next_exit(&mut self) -> Option<TaskExit> {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let joined = poll_fn(|cx| {
                eprintln!("scope D before next_exit state lock");
                let mut state = self.0.state.lock().expect("scope state poisoned");
                eprintln!("scope E acquired next_exit state lock");
                if let Some(exit) = state.completed.pop_front() {
                    return Poll::Ready(Some(exit));
                }
                eprintln!("scope F before actual JoinSet poll");
                match state.tasks.poll_join_next_with_id(cx) {
                    Poll::Ready(Some(result)) => { eprintln!("scope G joined actual task before exit record"); let exit=state.exit(result); eprintln!("scope H after actual exit record"); Poll::Ready(Some(exit)) },
                    Poll::Ready(None) if state.closing => Poll::Ready(None),
                    _ => Poll::Pending,
                }
            });
            tokio::select! { biased; exit = joined => return exit, _ = &mut changed => {} }
        }
    }

    pub fn begin_shutdown(&mut self) {
        eprintln!("scope A before state lock");
        self.0.state.lock().expect("scope state poisoned").closing = true;
        eprintln!("scope B before cancel");
        self.0.cancel.cancel();
        eprintln!("scope C after cancel");
        self.0.changed.notify_one();
    }

    /// Timeout retains task ownership; interruption also retains collected exits.
    /// This report proves task exit only; protocol I/O needs its owner's report.
    pub async fn shutdown(&mut self, deadline: Instant) -> CloseReport {
        self.begin_shutdown();
        loop {
            match timeout_at(deadline, self.next_exit()).await {
                Ok(Some(exit)) => {
                    self.0
                        .state
                        .lock()
                        .expect("scope state poisoned")
                        .record_closed(exit);
                }
                Ok(None) => {
                    return self
                        .0
                        .state
                        .lock()
                        .expect("scope state poisoned")
                        .take_close_report()
                }
                Err(_) => {
                    let mut state = self.0.state.lock().expect("scope state poisoned");
                    state.reap();
                    while let Some(exit) = state.completed.pop_front() {
                        state.record_closed(exit);
                    }
                    return state.take_close_report();
                }
            }
        }
    }
}

impl Drop for TaskScope {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("scope state poisoned");
        state.closing = true;
        state.tasks.abort_all();
        drop(state);
        self.0.cancel.cancel();
        // Emergency ownership cleanup only. This does not issue a CloseReport.
    }
}
