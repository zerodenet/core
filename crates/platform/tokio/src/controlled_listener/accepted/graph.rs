//! Bounded witnesses in the original accepted entry, not a task owner/JoinSet.
use super::*;
use crate::contract::scope::{
    TaskJoinWitness, TaskRegistrar, TaskScopeIdentity, WeakTaskRegistrar,
};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Weak,
    },
};
use zero_core::component::TaskId;
mod caller;
mod capacity;
mod peer;
pub use peer::AcceptedPeerScope;
mod carriers;
mod inbound;
mod receiver;
pub(super) mod resources;
pub use resources::AcceptedResource;
mod process;
mod spawn;
pub use caller::{AcceptedCaller, AcceptedOrigin};
pub(crate) use inbound::PendingTcp;
pub use receiver::AcceptedTcpTaskReceiver;

#[derive(Clone)]
pub(super) struct Authority {
    scope: TaskScopeIdentity,
    registrar: WeakTaskRegistrar,
    max_tasks: NonZeroUsize,
    pub(super) max_resources: usize,
}
impl Authority {
    pub(super) fn matches_original_scope(&self, scope: &TaskScopeIdentity) -> bool {
        self.scope.same_scope(scope)
    }

    pub(super) fn new(registrar: &TaskRegistrar, max_tasks: NonZeroUsize) -> Self {
        Self {
            scope: registrar.scope_identity(),
            registrar: registrar.downgrade(),
            max_tasks,
            max_resources: 0,
        }
    }
}
pub(super) struct Users {
    authority: Authority,
    peer: Option<Arc<peer::Ledger>>,
    entry: Weak<Entry>,
    pool: Weak<Pool>,
    cancel: CancellationToken,
    state: Mutex<State>,
    view_live: AtomicBool,
    changed: tokio::sync::Notify,
    logical_dirty: AtomicBool,
    carrier_cleanup: AsyncMutex<()>,
    local: OnceLock<std::net::SocketAddr>,
    pending_acks: Mutex<Vec<TaskJoinWitness>>,
    lookup_totals: OnceLock<(
        zero_core::component::ComponentInstanceId,
        Weak<crate::controlled_carrier::CarrierOriginalTotals>,
    )>,
}
#[derive(Default)]
struct State {
    primary: bool,
    spawning: usize,
    lookup_spawning: usize,
    sealed: bool,
    unaccounted: bool,
    tasks: BTreeMap<TaskId, Node>,
    resources: Vec<Arc<resources::Retained>>,
    carriers: Vec<carriers::CarrierUse>,
}
#[derive(Clone)]
struct Node {
    witness: TaskJoinWitness,
    primary: bool,
    process: Option<Arc<crate::LocalProcessLookup>>,
    process_helper: bool,
}
/// A unique byte view disappearing is independent of every actual task join.
pub(super) struct ViewUse(Arc<Users>);
impl Drop for ViewUse {
    fn drop(&mut self) {
        self.0.view_live.store(false, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}
/// Descendant registration in the exact already installed accepted entry/scope.
/// It retains neither its owner, original FD, protocol future nor task scope.
#[derive(Clone)]
pub struct AcceptedTasks {
    users: Weak<Users>,
    task: Arc<OnceLock<(TaskId, TaskJoinWitness)>>,
}
impl Users {
    pub(super) fn new(
        authority: Authority,
        entry: Weak<Entry>,
        pool: Weak<Pool>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            authority,
            peer: None,
            entry,
            pool,
            cancel,
            state: Mutex::new(State::default()),
            view_live: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
            logical_dirty: AtomicBool::new(false),
            carrier_cleanup: AsyncMutex::new(()),
            local: OnceLock::new(),
            pending_acks: Mutex::default(),
            lookup_totals: OnceLock::new(),
        }
    }
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn view(self: &Arc<Self>) -> ViewUse {
        assert!(
            !self.view_live.swap(true, Ordering::AcqRel),
            "one original accepted byte view"
        );
        ViewUse(self.clone())
    }
    pub(super) fn mark_unaccounted(&self) {
        self.state().unaccounted = true;
    }
    pub(super) fn status(&self) -> AcceptedTcpUsers {
        self.reap_process_lookups();
        let (sealed, pending, unaccounted, tasks, resources, carriers) = {
            let state = self.state();
            (
                state.sealed,
                state.spawning != 0,
                state.unaccounted,
                state
                    .tasks
                    .values()
                    .map(|node| node.witness.clone())
                    .collect::<Vec<_>>(),
                state.resources.clone(),
                !state.carriers.is_empty(),
            )
        };
        if unaccounted {
            return AcceptedTcpUsers::Unaccounted;
        }
        if !sealed
            || pending
            || self.view_live.load(Ordering::Acquire)
            || !tasks.is_empty()
            || resources.iter().any(|resource| !resource.complete())
            || carriers
        {
            AcceptedTcpUsers::Pending
        } else {
            AcceptedTcpUsers::Confirmed
        }
    }
    pub(super) fn complete(&self) -> bool {
        self.status() == AcceptedTcpUsers::Confirmed
    }
    pub(super) async fn retire(&self, deadline: Instant) -> bool {
        self.state().sealed = true;
        self.cancel.cancel();
        self.cancel_process_lookups();
        self.revoke_resources();
        self.revoke_carriers();
        let tasks: Vec<_> = self
            .state()
            .tasks
            .values()
            .map(|node| node.witness.clone())
            .collect();
        for task in tasks {
            if tokio::time::timeout_at(deadline, task.wait())
                .await
                .is_err()
            {
                return false;
            }
        }
        if !self.retire_process_lookups(deadline).await {
            return false;
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let ready = {
                let state = self.state();
                state.spawning == 0
                    && state.tasks.is_empty()
                    && !self.view_live.load(Ordering::Acquire)
            };
            if ready {
                if !self.close_resources(deadline).await {
                    return false;
                }
                if !self.close_carriers(deadline).await {
                    return false;
                }
                return self.complete();
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return false;
            }
        }
    }
    fn remember(&self, source: Arc<ChannelError>) {
        if let Some(peer) = &self.peer {
            peer.failure
                .lock()
                .unwrap()
                .get_or_insert_with(|| source.clone());
        }
        if let Some(entry) = self.entry.upgrade() {
            entry.state().failure.get_or_insert_with(|| source.clone());
        }
        if let Some(pool) = self.pool.upgrade() {
            pool.state().first_failure.get_or_insert(source);
        }
    }
}
impl AcceptedTcp {
    pub fn spawn_handler<F, Fut>(
        self,
        registrar: &TaskRegistrar,
        class: crate::contract::lifecycle::TaskClass,
        make: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(crate::TokioSocket, AcceptedTasks) -> Fut,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        let AcceptedTcp { io, claim } = self;
        let users = claim.entry.users.clone().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Unsupported,
                "accepted task graph was not declared",
            )
        })?;
        if !users.authority.scope.matches(registrar) {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "accepted handler belongs to another actual scope",
            ));
        }
        AcceptedTcp { io, claim }.spawn_handler_bound(class, make)
    }
    /// Register through the actual weak Scope authority frozen in this entry.
    /// The host installed that authority before bind; no caller-supplied Scope
    /// or strong Registrar is retained by the listener task future.
    pub fn spawn_handler_bound<F, Fut>(
        self,
        class: crate::contract::lifecycle::TaskClass,
        make: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(crate::TokioSocket, AcceptedTasks) -> Fut,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        let AcceptedTcp { io, claim } = self;
        let users = claim.entry.users.clone().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Unsupported,
                "accepted task graph was not declared",
            )
        })?;
        users.spawn(class, true, move |tasks| {
            make(crate::TokioSocket::from_accepted(io), tasks)
        })
    }
}
impl AcceptedTasks {
    /// The exact original registration, published before the first task poll.
    /// Its witness remains pending until the original Scope consumes the exit.
    pub fn current_task(&self) -> Option<(TaskId, TaskJoinWitness)> {
        self.task.get().cloned()
    }
    pub fn spawn<F, Fut>(
        &self,
        class: crate::contract::lifecycle::TaskClass,
        make: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(AcceptedTasks) -> Fut,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        let users = self.users.upgrade().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Revoked,
                "accepted task graph no longer exists",
            )
        })?;
        users.spawn(class, false, make)
    }
    /// Selected legacy/detached-user plans must explicitly retain this gap.
    pub fn mark_unaccounted(&self) {
        if let Some(users) = self.users.upgrade() {
            users.mark_unaccounted();
        }
    }
}
