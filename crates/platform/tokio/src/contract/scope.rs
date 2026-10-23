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
pub use zero_core::component::TaskId;

use super::{
    lifecycle::{CloseReport, ComponentError, TaskFailure, Unfinished},
    ChannelError, Impact, Reason, Stage,
};

pub use super::lifecycle::TaskClass;
mod join;
pub use join::{TaskJoinWitness, TaskJoined};
mod hosted;
mod identity;
mod registrar;
mod relay;
pub use hosted::{HostedCallbacks, HostedExitReceipt, HostedReceiptError, HostedScope};
pub use identity::TaskScopeIdentity;
pub use registrar::WeakTaskRegistrar;
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
    hosted: bool,
    registration_open: bool,
    closing: bool,
    limit: usize,
    tasks: JoinSet<Result<(), ComponentError>>,
    records: HashMap<Id, join::Registration>,
    completed: VecDeque<CompletedExit>,
    closing_report: Option<Box<CloseReport>>,
}

enum CompletedExit {
    Ordinary(TaskExit, Option<join::Notification>),
    Hosted(hosted::Completion, Option<join::Notification>),
}

struct Shared {
    state: Mutex<State>,
    changed: Arc<Notify>,
    cancel: CancellationToken,
    runtime: Handle,
    hosted_waker: Arc<Mutex<Option<std::task::Waker>>>,
    relay: Arc<relay::JoinRelay>,
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
    fn exit(
        &mut self,
        result: Result<(Id, Result<(), ComponentError>), JoinError>,
    ) -> CompletedExit {
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
        join::complete(record, result, self.hosted)
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
        scope.0.lock_state().registration_open = false;
        scope
    }

    /// Only the non-cloneable scope owner can grant registration. A retained
    /// protocol registrar cannot open build access or reopen a stopping scope.
    pub fn enable_registration(&mut self) -> Result<(), SpawnError> {
        let mut state = self.0.lock_state();
        if state.closing {
            return Err(SpawnError::Closing);
        }
        state.registration_open = true;
        Ok(())
    }

    fn with_limit(runtime: Handle, limit: usize) -> Self {
        Self::with_mode(runtime, limit, false)
    }

    fn with_mode(runtime: Handle, limit: usize, hosted: bool) -> Self {
        let changed = Arc::new(Notify::new());
        let hosted_waker = Arc::new(Mutex::new(None));
        Self(Arc::new_cyclic(|owner| Shared {
            state: Mutex::new(State {
                hosted,
                registration_open: true,
                closing: false,
                limit,
                tasks: JoinSet::new(),
                records: HashMap::new(),
                completed: VecDeque::new(),
                closing_report: None,
            }),
            relay: Arc::new(relay::JoinRelay::new(
                &changed,
                &hosted_waker,
                owner.clone(),
                hosted,
            )),
            changed,
            cancel: CancellationToken::new(),
            runtime,
            hosted_waker,
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
                self.0.register_hosted_waker(cx);
                let waker = std::task::Waker::from(self.0.relay.clone());
                let mut cx = std::task::Context::from_waker(&waker);
                let mut state = self.0.lock_state();
                if let Some(exit) = state.completed.pop_front() {
                    return Poll::Ready(Some(state.ordinary(exit)));
                }
                match state.tasks.poll_join_next_with_id(&mut cx) {
                    Poll::Ready(Some(result)) => {
                        let exit = state.exit(result);
                        Poll::Ready(Some(state.ordinary(exit)))
                    }
                    Poll::Ready(None) if state.closing => Poll::Ready(None),
                    _ => Poll::Pending,
                }
            });
            tokio::select! { biased; exit = joined => return exit, _ = &mut changed => {} }
        }
    }

    pub fn begin_shutdown(&mut self) {
        self.0.lock_state().closing = true;
        self.0.cancel.cancel();
        self.0.notify_hosted();
    }

    /// Timeout retains task ownership; interruption also retains collected exits.
    /// This report proves task exit only; protocol I/O needs its owner's report.
    pub async fn shutdown(&mut self, deadline: Instant) -> CloseReport {
        self.begin_shutdown();
        loop {
            match timeout_at(deadline, self.next_exit()).await {
                Ok(Some(exit)) => {
                    self.0.lock_state().record_closed(exit);
                }
                Ok(None) => return self.0.lock_state().take_close_report(),
                Err(_) => {
                    let mut state = self.0.lock_state();
                    state.reap();
                    while let Some(exit) = state.completed.pop_front() {
                        let exit = state.ordinary(exit);
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
        let mut state = self.0.lock_state();
        state.closing = true;
        state.tasks.abort_all();
        drop(state);
        let waker = self
            .0
            .hosted_waker
            .lock()
            .expect("scope waker poisoned")
            .take();
        drop(waker);
        self.0.cancel.cancel();
        // Emergency ownership cleanup only. This does not issue a CloseReport.
    }
}
