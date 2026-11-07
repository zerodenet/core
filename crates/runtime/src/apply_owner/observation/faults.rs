//! Fixed phases of this one original command, not a fault registry/executor.
use super::*;
use std::io;
use zero_platform_tokio::contract::ChannelError;
/// Only proof that this same original Work record currently retains a fault.
/// It grants no native, Task/Scope, ACK, cancellation or budget-release rights.
#[derive(Clone)]
pub struct CommandCleanupAccepted {
    receiver: std::sync::Weak<Observed>,
    source: std::sync::Weak<io::Error>,
    kind: CommandCleanupKind,
}
impl CommandCleanupAccepted {
    pub fn retained_source(&self) -> Option<Arc<io::Error>> {
        let receiver = self.receiver.upgrade()?;
        if !receiver.receives_faults {
            return None;
        }
        let original = self.source.upgrade()?;
        let same = receiver
            .state
            .lock()
            .expect("command observation poisoned")
            .faults
            .cleanup[self.kind.index()]
        .as_ref()
        .is_some_and(|source| Arc::ptr_eq(source, &original));
        same.then_some(original)
    }
}
#[derive(Clone, Copy, Debug)]
pub enum CommandCleanupKind {
    Probe,
    Resolution,
}
impl CommandCleanupKind {
    fn index(self) -> usize {
        match self {
            Self::Probe => 0,
            Self::Resolution => 1,
        }
    }
}
#[derive(Clone, Debug)]
pub struct BlockingCommandOutcome {
    id: tokio::task::Id,
    failure: Option<Arc<tokio::task::JoinError>>,
}
impl BlockingCommandOutcome {
    pub fn task_id(&self) -> tokio::task::Id {
        self.id
    }
    pub fn failure(&self) -> Option<&Arc<tokio::task::JoinError>> {
        self.failure.as_ref()
    }
}
#[derive(Default)]
pub(super) struct FaultState {
    blocking: Option<BlockingCommandOutcome>,
    cleanup: [Option<Arc<io::Error>>; 2],
}
#[derive(Clone, Debug)]
pub enum CommandInvocationFault {
    Apply(Arc<ChannelError>),
    Blocking(Arc<tokio::task::JoinError>),
    Cleanup(CommandCleanupKind, Arc<io::Error>),
}
impl std::fmt::Display for CommandInvocationFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Apply(e) => std::fmt::Display::fmt(e, f),
            Self::Blocking(e) => std::fmt::Display::fmt(e, f),
            Self::Cleanup(_, e) => std::fmt::Display::fmt(e, f),
        }
    }
}
impl std::error::Error for CommandInvocationFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Apply(e) => e.as_ref(),
            Self::Blocking(e) => e.as_ref(),
            Self::Cleanup(_, e) => e.as_ref(),
        })
    }
}
#[derive(Debug)]
pub struct CommandInvocationFaults {
    faults: [Option<CommandInvocationFault>; 4],
}
impl CommandInvocationFaults {
    /// Preserve the existing one-error payload/kind while retaining every
    /// independent original when several phases failed.
    pub fn into_io_error(self) -> io::Error {
        if self.faults.iter().flatten().count() == 1 {
            match self.faults.into_iter().flatten().next().unwrap() {
                CommandInvocationFault::Apply(source) => io::Error::other(source),
                CommandInvocationFault::Blocking(source) => io::Error::other(source),
                CommandInvocationFault::Cleanup(_, source) => io::Error::new(source.kind(), source),
            }
        } else {
            io::Error::other(self)
        }
    }
    pub fn faults(&self) -> impl Iterator<Item = &CommandInvocationFault> {
        self.faults.iter().flatten()
    }
}
impl std::fmt::Display for CommandInvocationFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for fault in self.faults() {
            write!(f, "; {fault}")?;
        }
        Ok(())
    }
}
impl std::error::Error for CommandInvocationFaults {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.faults().next().map(|fault| fault as _)
    }
}
impl ApplyObservation {
    /// Only known concrete cleanup phases publish here. A missing external Work
    /// receiver returns this same source to its canonical owner/library caller.
    /// Each production command invokes at most one operation of either kind.
    pub fn retain_cleanup_failure(
        kind: CommandCleanupKind,
        source: Arc<io::Error>,
    ) -> Result<CommandCleanupAccepted, Arc<io::Error>> {
        let Some(receiver) = Self::current().filter(|owner| owner.0.receives_faults) else {
            return Err(source);
        };
        let mut state = receiver
            .0
            .state
            .lock()
            .expect("command observation poisoned");
        let slot = &mut state.faults.cleanup[kind.index()];
        match slot {
            Some(old) if !Arc::ptr_eq(old, &source) => return Err(source),
            Some(_) => {}
            None => {
                *slot = Some(source.clone());
            }
        }
        drop(state);
        receiver.ownership_fault_published();
        Ok(CommandCleanupAccepted {
            receiver: Arc::downgrade(&receiver.0),
            source: Arc::downgrade(&source),
            kind,
        })
    }
    /// Called only after consuming this exact raw JoinHandle. It records that
    /// actual Tokio ID/result, never a TaskJoined/Scope/native closure receipt.
    pub fn record_blocking_outcome(
        id: tokio::task::Id,
        failure: Option<Arc<tokio::task::JoinError>>,
    ) -> Result<(), BlockingCommandOutcome> {
        let outcome = BlockingCommandOutcome { id, failure };
        if outcome
            .failure
            .as_ref()
            .is_some_and(|source| source.id() != id)
        {
            return Err(outcome);
        }
        let Some(receiver) = Self::current().filter(|owner| owner.0.receives_faults) else {
            return Err(outcome);
        };
        let mut state = receiver
            .0
            .state
            .lock()
            .expect("command observation poisoned");
        if state.faults.blocking.is_some() {
            return Err(outcome);
        }
        state.faults.blocking = Some(outcome);
        Ok(())
    }
    pub fn blocking_outcome(&self) -> Option<BlockingCommandOutcome> {
        self.0
            .state
            .lock()
            .expect("command observation poisoned")
            .faults
            .blocking
            .clone()
    }
    /// Called by the existing Work receiver after work/actual Apply join. Every
    /// source stays strongly typed and independent; taking a snapshot is no ACK.
    pub fn completion_fault(&self, joined: Option<&TaskJoined>) -> Option<CommandInvocationFaults> {
        let state = self.0.state.lock().expect("command observation poisoned");
        let faults = [
            joined
                .and_then(|joined| joined.result().err())
                .map(CommandInvocationFault::Apply),
            state
                .faults
                .blocking
                .as_ref()
                .and_then(|o| o.failure.clone())
                .map(CommandInvocationFault::Blocking),
            state.faults.cleanup[0]
                .clone()
                .map(|e| CommandInvocationFault::Cleanup(CommandCleanupKind::Probe, e)),
            state.faults.cleanup[1]
                .clone()
                .map(|e| CommandInvocationFault::Cleanup(CommandCleanupKind::Resolution, e)),
        ];
        faults
            .iter()
            .any(Option::is_some)
            .then_some(CommandInvocationFaults { faults })
    }
}
