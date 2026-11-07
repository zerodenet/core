//! One actual controller's logical completion and retained continuation only.
//! No constructor from IDs/bools, native close, task permission or Scope proof.
use super::*;
use crate::contract::{ChannelError, CleanupFailureReceipt, DeliveryState, Impact, Reason, Stage};
use std::{
    error::Error,
    fmt,
    sync::{atomic::Ordering, OnceLock, Weak},
};
#[derive(Debug, Default)]
struct Causes {
    deadline: OnceLock<Arc<ChannelError>>,
    terminal: OnceLock<Arc<ChannelError>>,
}
impl fmt::Display for Causes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("same logical controller cleanup causes")
    }
}
impl Error for Causes {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.terminal
            .get()
            .or_else(|| self.deadline.get())
            .map(|same| same.as_ref() as &(dyn Error + 'static))
    }
}
pub(super) struct Observation {
    armed: AtomicBool,
    complete: AtomicBool,
    alive: AtomicBool,
    causes: Arc<Causes>,
    receipt: CleanupFailureReceipt,
    report: OnceLock<Arc<ChannelError>>,
}
/// Opaque capability bound to the unique original LoanState. Assembly moves it
/// to that same concrete RawTCP/UDP controller before any IO view escapes.
/// No Host ResourceAccess/metadata/ID interface can create a participant.
pub struct LogicalLoanCloseParticipant {
    observation: Arc<Observation>,
    state: Weak<State>,
}
impl LoanOwner {
    /// One-time split of this actual logical controller's existing authority.
    /// The owning adapter installs it in its original control before first IO;
    /// Host observers retain only this owner's readonly completion observation.
    pub fn take_logical_close_participant(
        &mut self,
    ) -> Result<LogicalLoanCloseParticipant, ChannelError> {
        if self.native_control_only
            || self.logical_control.is_some()
            || self.closed
            || self.state.returned.load(Ordering::Acquire)
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        let causes = Arc::new(Causes::default());
        let receipt = CleanupFailureReceipt::new();
        let payload: Arc<dyn Error + Send + Sync> = causes.clone();
        assert!(receipt.bind_payload(payload));
        let observation = Arc::new(Observation {
            armed: false.into(),
            complete: false.into(),
            alive: true.into(),
            causes,
            receipt,
            report: OnceLock::new(),
        });
        self.logical_control = Some(observation.clone());
        Ok(LogicalLoanCloseParticipant {
            observation,
            state: Arc::downgrade(&self.state),
        })
    }
    /// Only the bound controller's original already-started continuation; this
    /// does not allow a failed native operation or another cleanup action.
    pub(crate) fn logical_cleanup_continuation(&self) -> bool {
        self.logical_control.as_ref().is_some_and(|same| {
            same.alive.load(Ordering::Acquire)
                && same.armed.load(Ordering::Acquire)
                && !same.complete.load(Ordering::Acquire)
                && same.causes.terminal.get().is_none()
        })
    }
    pub(super) fn observed_logical_closed(&self) -> bool {
        self.state.returned.load(Ordering::Acquire)
            && self
                .logical_control
                .as_ref()
                .is_some_and(|same| same.complete.load(Ordering::Acquire))
    }
}
impl LogicalLoanCloseParticipant {
    /// Called after the owning control has installed its one actual close
    /// future/typed original receipt; re-entry only polls that same future.
    pub fn continuation_started(&self) {
        self.observation.armed.store(true, Ordering::Release);
    }
    /// The private original controller calls this only after its exact typed
    /// socket close receipt and all its admitted sends have really settled.
    pub fn confirm(&self) {
        if !self.observation.armed.load(Ordering::Acquire) {
            return;
        }
        self.observation.complete.store(true, Ordering::Release);
        if let Some(state) = self.state.upgrade() {
            state.changed.notify_waiters();
        }
    }
    pub fn deadline(&self, source: tokio::time::error::Elapsed) -> ChannelError {
        let actual = self.observation.causes.deadline.get_or_init(|| {
            let mut error = ChannelError::new(Stage::Close, Reason::Timeout, Impact::Session)
                .with_source(source);
            error.delivery = DeliveryState::Unknown;
            Arc::new(error)
        });
        self.report(actual)
    }
    pub fn terminal(&self, source: Arc<ChannelError>) -> ChannelError {
        let actual = self.observation.causes.terminal.get_or_init(|| source);
        self.report(actual)
    }
    fn report(&self, actual: &Arc<ChannelError>) -> ChannelError {
        let report = self.observation.report.get_or_init(|| {
            Arc::new(
                ChannelError::retained(actual.clone())
                    .with_source(self.observation.causes.clone())
                    .with_cleanup_failure_receipt(self.observation.receipt.clone()),
            )
        });
        ChannelError::retained(report.clone())
    }
}
impl Drop for LogicalLoanCloseParticipant {
    fn drop(&mut self) {
        self.observation.alive.store(false, Ordering::Release);
        if let Some(state) = self.state.upgrade() {
            state.changed.notify_waiters();
        }
    }
}
