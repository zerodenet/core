//! Same budget transaction fences the actual raw-command Scope, not the
//! frontend connection/RPC Scope. A no-raw configuration request remains charged
//! while its independent original Apply and caller continue to completion.
use super::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Weak,
};
use zero_platform_tokio::contract::scope::ScopeRetirementObservation;
pub(crate) struct CommandScopePause {
    owner: Weak<EmbeddedCommandOwner>,
    identity: Weak<()>,
    original: Option<ScopeRetirementObservation>,
    complete: AtomicBool,
    started: AtomicBool,
}
impl EmbeddedCommandOwner {
    pub(crate) fn pause_for_limit(
        self: &Arc<Self>,
        limit: usize,
        identity: &Arc<()>,
    ) -> std::io::Result<Option<Arc<CommandScopePause>>> {
        let mut state = lock(&self.state);
        if state.limit == 0 || state.limit == limit {
            return Ok(None);
        }
        if state.closing || state.faulted || state.pause.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "actual command Scope is unavailable",
            ));
        }
        if !raw_scope_quiescent(&state) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "actual command raw/cleanup registrations remain live",
            ));
        }
        let original = state
            .tasks
            .as_ref()
            .map(TaskRegistrar::retirement_observation);
        state.pause = Some(Arc::downgrade(identity));
        Ok(Some(Arc::new(CommandScopePause {
            owner: Arc::downgrade(self),
            identity: Arc::downgrade(identity),
            original,
            complete: AtomicBool::new(false),
            started: AtomicBool::new(false),
        })))
    }
}
impl CommandScopePause {
    pub(crate) fn started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }
    pub(crate) fn complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
    pub(crate) async fn retire(&self, end: Instant) -> Result<(), ChannelError> {
        self.finish(Some(end)).await
    }
    /// Continue only the already requested original cleanup. The caller keeps
    /// its original timeout result; this never starts another task or Scope.
    pub(crate) async fn finish_original(&self) -> Result<(), ChannelError> {
        if !self.started() {
            return Ok(());
        }
        self.finish(None).await
    }
    async fn finish(&self, deadline: Option<Instant>) -> Result<(), ChannelError> {
        if self.complete() {
            return Ok(());
        }
        let owner = self.owner.upgrade().ok_or_else(|| fault(Reason::Revoked))?;
        let _serial = owner.serial.lock().await;
        let stop = {
            let mut state = lock(&owner.state);
            if !state
                .pause
                .as_ref()
                .is_some_and(|id| id.ptr_eq(&self.identity))
                || !raw_scope_quiescent(&state)
            {
                return Err(fault(Reason::StalePlan));
            }
            state.scope_retiring = true;
            state.monitor_stop.clone()
        };
        self.started.store(true, Ordering::Release);
        stop.cancel();
        owner.changed.notify_waiters();
        match deadline {
            Some(end) => timeout_at(end, owner.finish_monitor())
                .await
                .map_err(|e| fault(Reason::Timeout).with_source(e))??,
            None => owner.finish_monitor().await?,
        }
        if self
            .original
            .as_ref()
            .is_some_and(|scope| !scope.is_drained())
        {
            return Err(fault(Reason::Timeout));
        }
        let (scope, tasks) = {
            let mut state = lock(&owner.state);
            let old = (state.scope.take(), state.tasks.take());
            state.monitor = None;
            state.limit = 0;
            state.scope_retiring = false;
            old
        };
        drop(scope);
        drop(tasks);
        self.complete.store(true, Ordering::Release);
        owner.original_failure().map_or(Ok(()), Err)
    }
    /// Only the existing budget token's commit/discard calls this. Dropping a
    /// caller future or the token does not unpause an unresolved transaction.
    pub(crate) fn release(&self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut state = lock(&owner.state);
            if state
                .pause
                .as_ref()
                .is_some_and(|id| id.ptr_eq(&self.identity))
            {
                state.pause = None;
            }
            let restart = state.tasks.is_none()
                && !state.closing
                && state.frontends.iter().any(|e| e.has_credit());
            drop(state);
            owner.changed.notify_waiters();
            if restart {
                if let Some(book) = owner.book.get().and_then(Weak::upgrade) {
                    // Same original runtime, only the declared receipt duty.
                    // A concurrent initializer/closing Host owns its own outcome.
                    let _ = owner.ensure_now(book.limits().messages.pending_commands);
                }
            }
        }
    }
}
pub(super) fn raw_scope_quiescent(state: &State) -> bool {
    state.entries.iter().all(|e| lock(&e.credit).is_none())
        && state.frontends.iter().all(|e| e.raw_scope_returned())
}

impl std::fmt::Debug for CommandScopePause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandScopePause").finish_non_exhaustive()
    }
}
