//! This same finite Host duty retains embedded work and frontend continuations.
//! Frontend actors keep their original Scope; actual raw children alone register
//! here, and its existing monitor retains cleanup when a caller disappears.
use crate::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
use crate::ApplyObservation;
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::{
    sync::{oneshot, Notify},
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{
    scope::{HostedScope, TaskClass, TaskJoinWitness, TaskRegistrar, TaskScope},
    CancellationToken, ChannelError, Impact, Reason, Stage,
};
pub(crate) mod frontend;
pub use frontend::FrontendCommand;
mod async_panic;
mod async_run;
mod initialize;
pub(crate) mod limits;
mod receipts;
mod run;
pub use async_panic::AsyncCommandPanic;
pub use run::CommandTaskFaults as BlockingCommandFaults;

#[derive(Default)]
pub struct EmbeddedCommandOwner {
    state: Mutex<State>,
    serial: tokio::sync::Mutex<()>,
    changed: Notify,
    book: OnceLock<std::sync::Weak<ControlBudget>>,
    runtime: OnceLock<tokio::runtime::Handle>,
    #[cfg(test)]
    hold_ack: Mutex<Option<oneshot::Receiver<()>>>,
    #[cfg(test)]
    observation_gate: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}
#[derive(Default)]
struct State {
    // The actual monitor owns this exact Scope while running. It returns it
    // before exit so only an external Host consumer can join that monitor.
    scope: Option<HostedScope>,
    tasks: Option<TaskRegistrar>,
    monitor: Option<TaskJoinWitness>,
    monitor_stop: CancellationToken,
    entries: Vec<Arc<Entry>>,
    frontends: Vec<Arc<frontend::Original>>,
    limit: usize,
    closing: bool,
    faulted: bool,
    pause: Option<std::sync::Weak<()>>,
    scope_retiring: bool,
    starting: bool,
    monitor_failure: Option<Arc<ChannelError>>,
}
pub(crate) struct Entry {
    owner: std::sync::Weak<EmbeddedCommandOwner>,
    observation: ApplyObservation,
    stop: CancellationToken,
    raw: OnceLock<TaskJoinWitness>,
    wrapper: OnceLock<TaskJoinWitness>,
    raw_id: OnceLock<tokio::task::Id>,
    credit: Mutex<Option<ControlBudgetPermit>>,
    failure: OnceLock<Arc<ChannelError>>,
    raw_failure: OnceLock<Arc<ChannelError>>,
    wrapper_rejected: std::sync::atomic::AtomicBool,
}
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn fault(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Close, reason, Impact::Component)
}
fn retained(source: Arc<ChannelError>) -> ChannelError {
    ChannelError::retained(source)
}
impl std::fmt::Debug for EmbeddedCommandOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedCommandOwner")
            .finish_non_exhaustive()
    }
}
impl EmbeddedCommandOwner {
    pub(crate) fn ownership_fault_published(&self) {
        lock(&self.state).faulted = true;
        self.changed.notify_waiters();
    }
    pub(crate) fn bind_book(&self, book: &Arc<ControlBudget>) {
        assert!(
            self.book.set(Arc::downgrade(book)).is_ok(),
            "one actual control book"
        );
    }

    pub fn validate_limit(&self, limit: usize) -> std::io::Result<()> {
        let state = lock(&self.state);
        if state.limit != 0 && state.limit != limit && !limits::raw_scope_quiescent(&state) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "actual command raw/cleanup registrations remain live",
            ));
        }
        Ok(())
    }
    pub(crate) fn original_failure(&self) -> Option<ChannelError> {
        let state = lock(&self.state);
        let mut faults = state
            .entries
            .iter()
            .filter_map(|entry| {
                let raw = entry.raw_failure.get().cloned();
                let wrapper = entry.failure.get().cloned().or_else(|| {
                    entry
                        .observation
                        .completion_fault(None)
                        .map(|source| Arc::new(fault(Reason::Internal).with_source(source)))
                });
                (raw.is_some() || wrapper.is_some()).then_some(EmbeddedFaults { raw, wrapper })
            })
            .collect::<Vec<_>>();
        faults.extend(state.frontends.iter().filter_map(|e| e.faults()));
        if let Some(error) = &state.monitor_failure {
            faults.push(EmbeddedFaults {
                raw: None,
                wrapper: Some(error.clone()),
            });
        }
        (!faults.is_empty()).then(|| fault(Reason::Internal).with_source(EmbeddedFaultSet(faults)))
    }
    pub fn begin_shutdown(&self) {
        let (stops, monitor) = {
            let mut state = lock(&self.state);
            state.closing = true;
            (
                state
                    .entries
                    .iter()
                    .map(|e| e.stop.clone())
                    .chain(state.frontends.iter().filter_map(|e| e.stop_token()))
                    .collect::<Vec<_>>(),
                state.monitor_stop.clone(),
            )
        };
        for stop in stops {
            stop.cancel();
        }
        monitor.cancel();
        self.changed.notify_waiters();
    }
    /// Physical receipts are independent of a retained historical error.
    pub fn retirement_complete(&self) -> bool {
        let state = lock(&self.state);
        state.entries.iter().all(|e| lock(&e.credit).is_none())
            && state.frontends.iter().all(|e| !e.has_credit())
            && state
                .scope
                .as_ref()
                .map_or(state.tasks.is_none(), HostedScope::is_drained)
    }
    pub async fn shutdown(&self, end: Instant) -> Result<(), ChannelError> {
        self.begin_shutdown();
        let _serial = self.serial.lock().await;
        timeout_at(end, self.finish_monitor())
            .await
            .map_err(|e| fault(Reason::Timeout).with_source(e))??;
        timeout_at(
            end,
            std::future::poll_fn(|cx| {
                let _ = self.poll_frontends(cx);
                if lock(&self.state).frontends.iter().all(|e| !e.has_credit()) {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            }),
        )
        .await
        .map_err(|source| fault(Reason::Timeout).with_source(source))?;
        let returned = self.original_failure();
        if self.retirement_complete() {
            // Construct the actual returned typed receiver before releasing any
            // bounded historical row. No error is converted into a close proof.
            let old = std::mem::take(&mut lock(&self.state).entries);
            drop(old);
            let old = std::mem::take(&mut lock(&self.state).frontends);
            drop(old);
        }
        returned.map_or(Ok(()), Err)
    }
    pub fn reopen_after_retirement(&self) -> std::io::Result<()> {
        if !lock(&self.state).closing {
            return Ok(());
        }
        if !self.retirement_complete() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "embedded command Scope remains live",
            ));
        }
        let mut state = lock(&self.state);
        if !state.entries.is_empty() || !state.frontends.is_empty() {
            return Err(std::io::Error::other(
                "embedded faults have not reached their shutdown receiver",
            ));
        }
        state.closing = false;
        state.faulted = false;
        state.monitor_failure = None;
        state.tasks = None;
        state.monitor = None;
        let old = state.scope.take();
        state.limit = 0;
        drop(state);
        drop(old);
        Ok(())
    }
}
#[cfg(test)]
#[path = "embedded_commands/tests.rs"]
mod tests;

#[derive(Debug)]
pub struct EmbeddedFaults {
    raw: Option<Arc<ChannelError>>,
    wrapper: Option<Arc<ChannelError>>,
}
impl std::fmt::Display for EmbeddedFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "embedded original faults: {:?}; {:?}",
            self.raw, self.wrapper
        )
    }
}
impl std::error::Error for EmbeddedFaults {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.raw
            .as_ref()
            .or(self.wrapper.as_ref())
            .map(|e| e.as_ref() as _)
    }
}

#[derive(Debug)]
pub struct EmbeddedFaultSet(Vec<EmbeddedFaults>);
impl std::fmt::Display for EmbeddedFaultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for source in &self.0 {
            write!(f, "; {source}")?;
        }
        Ok(())
    }
}
impl std::error::Error for EmbeddedFaultSet {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|e| e as _)
    }
}

impl Entry {
    pub(crate) fn ownership_fault_published(&self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.ownership_fault_published();
        }
    }
}

impl EmbeddedFaults {
    pub fn raw(&self) -> Option<&Arc<ChannelError>> {
        self.raw.as_ref()
    }
    pub fn wrapper(&self) -> Option<&Arc<ChannelError>> {
        self.wrapper.as_ref()
    }
}
impl EmbeddedFaultSet {
    pub fn faults(&self) -> &[EmbeddedFaults] {
        &self.0
    }
}
