//! Actual frontend records transfer their original permission into the same
//! Host owner. A public observation or a bare counter permit is never a Work.
use super::*;
use crate::configuration::{ControlCommandCustody, ControlConnectionPermit};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Weak,
};
use zero_platform_tokio::contract::BoxFuture;
mod async_run;
mod drive;
mod registration;
mod run;

#[derive(Clone)]
pub struct FrontendCommand(Arc<Original>);
pub(crate) struct Original {
    owner: Weak<EmbeddedCommandOwner>,
    custody: ControlCommandCustody,
    observation: ApplyObservation,
    registration: OnceLock<Registration>,
    stop: OnceLock<CancellationToken>,
    raw: OnceLock<TaskJoinWitness>,
    raw_id: OnceLock<tokio::task::Id>,
    creating: AtomicBool,
    cleanup: Mutex<Option<BoxFuture<'static, Result<(), ChannelError>>>>,
    cleanup_complete: AtomicBool,
    caller_returned: AtomicBool,
    polling: AtomicBool,
    poisoned: AtomicBool,
    caller_ack: Mutex<Option<BoxFuture<'static, ()>>>,
    failure: OnceLock<Arc<ChannelError>>,
    raw_failure: OnceLock<Arc<ChannelError>>,
    credit: Mutex<Option<Credit>>,
    request_bytes: Mutex<Vec<ControlBudgetPermit>>,
    raw_no_work: AtomicBool,
    async_work: AtomicBool,
}
enum Credit {
    Root { _original: ControlBudgetPermit },
    Connection(ControlConnectionPermit),
}
enum Registration {
    Scoped(TaskJoinWitness),
    JoinSet(tokio::task::AbortHandle),
    Inline(tokio::task::AbortHandle),
}
impl EmbeddedCommandOwner {
    pub fn adopt_command(
        self: &Arc<Self>,
        credit: ControlBudgetPermit,
    ) -> Result<FrontendCommand, std::io::Error> {
        let proof = credit
            .command_custody()
            .ok_or_else(|| std::io::Error::other("not an actual command permission"))?;
        self.adopt(Credit::Root { _original: credit }, proof, Vec::new())
    }
    pub fn adopt_connection_command(
        self: &Arc<Self>,
        credit: ControlConnectionPermit,
    ) -> Result<FrontendCommand, std::io::Error> {
        let proof = credit
            .command_custody()
            .ok_or_else(|| std::io::Error::other("not an actual local command permission"))?;
        self.adopt(Credit::Connection(credit), proof, Vec::new())
    }
    /// Transfer the original IPC request envelope's existing byte permissions.
    /// Caller loss cannot refund bytes still held by its actual blocking job.
    pub fn adopt_connection_command_with_input(
        self: &Arc<Self>,
        credit: ControlConnectionPermit,
        request_bytes: Vec<ControlBudgetPermit>,
    ) -> Result<FrontendCommand, std::io::Error> {
        let proof = credit
            .command_custody()
            .ok_or_else(|| std::io::Error::other("not an actual local command permission"))?;
        self.adopt(Credit::Connection(credit), proof, request_bytes)
    }
    fn adopt(
        self: &Arc<Self>,
        credit: Credit,
        custody: ControlCommandCustody,
        request_bytes: Vec<ControlBudgetPermit>,
    ) -> Result<FrontendCommand, std::io::Error> {
        let root = self
            .book
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| std::io::Error::other("command owner has no original Host book"))?;
        if !custody.matches(&root)
            || request_bytes
                .iter()
                .any(|permit| !permit.request_owner_matches(&root))
        {
            return Err(std::io::Error::other("foreign command permission"));
        }
        self.ensure_now(root.limits().messages.pending_commands)
            .map_err(std::io::Error::other)?;
        let observation = ApplyObservation::new();
        let original = Arc::new(Original {
            owner: Arc::downgrade(self),
            custody,
            observation: observation.clone(),
            registration: OnceLock::new(),
            stop: OnceLock::new(),
            raw: OnceLock::new(),
            raw_id: OnceLock::new(),
            creating: AtomicBool::new(false),
            cleanup: Mutex::new(None),
            cleanup_complete: AtomicBool::new(false),
            caller_returned: AtomicBool::new(false),
            polling: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            caller_ack: Mutex::new(None),
            failure: OnceLock::new(),
            raw_failure: OnceLock::new(),
            credit: Mutex::new(Some(credit)),
            request_bytes: Mutex::new(request_bytes),
            raw_no_work: AtomicBool::new(false),
            async_work: AtomicBool::new(false),
        });
        observation.install_frontend(Arc::downgrade(&original));
        let mut state = lock(&self.state);
        if state.closing
            || state.faulted
            || state.pause.is_some()
            || state
                .frontends
                .iter()
                .any(|e| e.failure.get().is_some() || e.raw_failure.get().is_some())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "original command admission closed",
            ));
        }
        state.frontends.push(original.clone());
        drop(state);
        Ok(FrontendCommand(original))
    }
}
impl Original {
    pub(super) fn raw_scope_returned(&self) -> bool {
        match self.raw.get() {
            Some(raw) => {
                raw.try_joined()
                    .is_some_and(|joined| joined.registration_released())
                    && self.cleanup_complete.load(Ordering::Acquire)
            }
            None => {
                !self.creating.load(Ordering::Acquire)
                    && (!self.async_work.load(Ordering::Acquire)
                        || self.cleanup_complete.load(Ordering::Acquire))
            }
        }
    }
    pub(super) fn raw_witness(&self) -> Option<&TaskJoinWitness> {
        self.raw.get()
    }
    pub(super) fn record_raw_failure(&self, source: Arc<ChannelError>) {
        self.raw_failure.get_or_init(|| source);
    }

    pub(super) fn has_credit(&self) -> bool {
        lock(&self.credit).is_some()
    }
    pub(super) fn stop_token(&self) -> Option<CancellationToken> {
        self.stop.get().cloned()
    }
    pub(super) fn faults(&self) -> Option<EmbeddedFaults> {
        let raw = self.raw_failure.get().cloned();
        let wrapper = self.failure.get().cloned().or_else(|| {
            self.observation
                .completion_fault(None)
                .map(|source| Arc::new(fault(Reason::Internal).with_source(source)))
        });
        (raw.is_some() || wrapper.is_some()).then_some(EmbeddedFaults { raw, wrapper })
    }

    pub(crate) fn ownership_fault_published(&self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.ownership_fault_published();
        }
    }
    fn notify(&self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.changed.notify_waiters();
        }
    }
    pub(crate) fn attach_stop(&self, stop: CancellationToken) {
        let _ = self.stop.set(stop);
    }
    pub(crate) fn matches_current(&self, root: &Arc<ControlBudget>) -> bool {
        if !self.custody.matches(root) || lock(&self.credit).is_none() {
            return false;
        }
        let current = tokio::task::try_id();
        match self.registration.get() {
            Some(Registration::Scoped(actual)) => {
                actual.native_task_id() == current && !actual.is_joined()
            }
            Some(Registration::JoinSet(actual) | Registration::Inline(actual)) => {
                Some(actual.id()) == current && !actual.is_finished()
            }
            None => false,
        }
    }
    async fn finish_unstarted(&self) -> Result<(), ChannelError> {
        if self.raw.get().is_some() || self.async_work.load(Ordering::Acquire) {
            loop {
                let owner = self.owner.upgrade().ok_or_else(|| fault(Reason::Revoked))?;
                let changed = owner.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.cleanup_complete.load(Ordering::Acquire) {
                    break;
                }
                changed.await;
            }
        }
        if self.raw.get().is_none()
            && !self.async_work.load(Ordering::Acquire)
            && !self.creating.load(Ordering::Acquire)
        {
            let joined = self.observation.wait_joined().await;
            if let Some(source) = self.observation.completion_fault(joined.as_ref()) {
                self.failure
                    .get_or_init(|| Arc::new(fault(Reason::Internal).with_source(source)));
            }
            self.cleanup_complete.store(true, Ordering::Release);
        }
        Ok(())
    }
}

#[cfg(test)]
impl FrontendCommand {
    pub(super) fn test_async_started(&self) -> bool {
        self.0.async_work.load(Ordering::Acquire)
    }
    pub(super) fn test_async_returned(&self) -> bool {
        self.0.raw_scope_returned()
    }
    pub(super) fn test_async_failure(&self) -> Option<Arc<ChannelError>> {
        self.0.raw_failure.get().cloned()
    }
    pub(super) fn test_blocking_id(&self) -> Option<tokio::task::Id> {
        self.0.raw_id.get().copied()
    }
    pub(super) fn test_raw_witness(&self) -> Option<TaskJoinWitness> {
        self.0.raw.get().cloned()
    }
}
