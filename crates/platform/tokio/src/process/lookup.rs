//! One existing blocking worker. The containing accepted/peer ledger retains it.
use super::{LocalProcessInfo, SocketAddr};
use crate::contract::{
    scope::{SpawnError, TaskClass, TaskJoinWitness, WeakTaskRegistrar},
    CancellationToken, ChannelError, DeliveryState, Impact, Reason, Stage,
};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
mod native;
mod registration;
mod totals;
use native::Native;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalProcessTransport {
    Tcp,
    Udp,
}
type Outcome = Result<Option<LocalProcessInfo>, Arc<ChannelError>>;
pub struct LocalProcessLookup {
    source: SocketAddr,
    transport: LocalProcessTransport,
    caller: TaskJoinWitness,
    owner_cancel: CancellationToken,
    read_limit: std::num::NonZeroUsize,
    pub(in crate::process) native: Native,
    witness: Arc<OnceLock<TaskJoinWitness>>,
    state: Mutex<State>,
    start: Condvar,
    changed: Arc<tokio::sync::Notify>,
    totals: totals::Custody,
}
struct State {
    attempted: bool,
    rejected: bool,
    admitted: bool,
    cancelled: bool,
    result: Option<Outcome>,
    #[cfg(test)]
    waiting_at_start: bool,
}
/// Release only after storing the original and actual witness in its ledger.
/// Drop stops an unstarted worker; it never reports a join or native close.
pub struct LocalProcessLookupStart {
    original: Arc<LocalProcessLookup>,
    started: bool,
}
impl LocalProcessLookupStart {
    pub fn start(mut self) {
        self.original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .admitted = true;
        self.started = true;
        self.original.start.notify_all();
    }
}
impl Drop for LocalProcessLookupStart {
    fn drop(&mut self) {
        if !self.started {
            self.original.cancel();
        }
    }
}
impl LocalProcessLookup {
    /// Cold concrete metadata only. An observed identity cannot create a worker.
    pub fn prepare(
        caller: TaskJoinWitness,
        source: SocketAddr,
        transport: LocalProcessTransport,
        owner_cancel: CancellationToken,
        read_limit: std::num::NonZeroUsize,
    ) -> Result<Arc<Self>, ChannelError> {
        read_limit
            .get()
            .checked_add(1)
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        Ok(Arc::new(Self {
            source,
            transport,
            caller,
            owner_cancel,
            read_limit,
            native: Native::new(),
            witness: Arc::new(OnceLock::new()),
            state: Mutex::new(State {
                attempted: false,
                rejected: false,
                admitted: false,
                cancelled: false,
                result: None,
                #[cfg(test)]
                waiting_at_start: false,
            }),
            start: Condvar::new(),
            changed: Arc::new(tokio::sync::Notify::new()),
            totals: totals::Custody::default(),
        }))
    }
    pub(in crate::process) fn read_limit(&self) -> usize {
        self.read_limit.get()
    }
    pub(in crate::process) fn source(&self) -> SocketAddr {
        self.source
    }
    pub(in crate::process) fn transport(&self) -> LocalProcessTransport {
        self.transport
    }
    pub(in crate::process) fn cancelled(&self) -> bool {
        self.owner_cancel.is_cancelled()
            || self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .cancelled
    }
    /// Only the same actual Scope's joined worker permits native finalization.
    pub fn collect_native_after_join(&self) -> Result<(), ChannelError> {
        let witness = self
            .witness
            .get()
            .ok_or_else(|| error(Stage::Close, Reason::Revoked))?;
        let joined = witness
            .try_joined()
            .ok_or_else(|| error(Stage::Close, Reason::Revoked))?;
        if !witness.matches_joined(&joined) {
            return Err(error(Stage::Close, Reason::StalePlan));
        }
        self.native.collect_after_join().map_err(retained)
    }
    pub fn caller(&self) -> &TaskJoinWitness {
        &self.caller
    }
    pub fn witness(&self) -> Option<TaskJoinWitness> {
        self.witness.get().cloned()
    }
    pub fn cancel(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancelled = true;
        self.start.notify_all();
        self.changed.notify_waiters();
    }
    fn run(&self) -> Result<(), ChannelError> {
        let admitted = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !state.admitted && !state.cancelled {
                #[cfg(test)]
                {
                    state.waiting_at_start = true;
                }
                state = self
                    .start
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                #[cfg(test)]
                {
                    state.waiting_at_start = false;
                }
            }
            state.admitted && !state.cancelled && !self.owner_cancel.is_cancelled()
        };
        let result = if !admitted {
            Ok(None)
        } else {
            super::platform::lookup(self)
        };
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .result = Some(result.clone());
        self.changed.notify_waiters();
        result.map(|_| ()).map_err(retained)
    }
    /// A result is metadata only, never a native close/Scope receipt/refund.
    pub async fn result(
        &self,
        cancel: &CancellationToken,
    ) -> Result<Option<LocalProcessInfo>, ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(result) = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .result
                .clone()
            {
                return result.map_err(retained);
            }
            let witness = self
                .witness
                .get()
                .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
            tokio::select! { biased;
                _ = cancel.cancelled() => { self.cancel(); return Err(error(Stage::Prepare, Reason::Revoked)); }
                joined = witness.wait() => {
                    // A panic may bypass the body's metadata publication. Only
                    // the original consumed Scope join supplies this failure.
                    if let Err(source) = joined.result() { return Err(retained(source)); }
                    if self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).result.is_none() { return Err(error(Stage::Prepare, Reason::Io)); }
                }
                _ = changed => {},
            }
        }
    }
    pub fn retired(&self) -> bool {
        let joined = self.witness.get().and_then(TaskJoinWitness::try_joined);
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.native.confirmed()
            && (state.rejected || joined.is_some_and(|joined| joined.registration_released()))
    }
    pub async fn wait_retired(&self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        if self.retired() {
            return Ok(());
        }
        let witness = self
            .witness
            .get()
            .ok_or_else(|| error(Stage::Close, Reason::Io))?;
        let joined = tokio::time::timeout_at(deadline, witness.wait_registration_released())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        self.collect_native_after_join()?;
        if let Some(source) = self.native.failure() {
            return Err(retained(source));
        }
        if !self.native.confirmed() {
            return Err(joined
                .result()
                .err()
                .map_or_else(|| error(Stage::Close, Reason::Io), retained));
        }
        Ok(())
    }
}
fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Session)
}
fn retained(source: Arc<ChannelError>) -> ChannelError {
    ChannelError::retained(source)
}

#[cfg(test)]
mod tests;
