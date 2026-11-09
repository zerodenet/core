//! One concrete controlled TUN helper job graph per Source, NOT a process tree.
//! Foreground authorization, fixed helper/tools, pipes, transfer and kqueue
//! originals stay charged through all recovery epochs and actual Scope ACKs.
use crate::contract::scope::{ScopeRetirementCompletion, WeakTaskRegistrar};
use std::{
    io,
    sync::{Arc, Mutex},
    task::Context,
};
use zero_core::component::ComponentInstanceId;
use zero_tun::packet_memory::{PacketDriverMetadataView, PacketMetadataArc, PacketMetadataVec};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessBudgetLimits {
    pub max_jobs: usize,
    pub max_jobs_per_instance: usize,
}
impl Default for ProcessBudgetLimits {
    fn default() -> Self {
        Self {
            max_jobs: 64,
            max_jobs_per_instance: 1,
        }
    }
}
impl ProcessBudgetLimits {
    pub fn validate(self) -> io::Result<Self> {
        if self.max_jobs == 0
            || self.max_jobs_per_instance == 0
            || self.max_jobs_per_instance > self.max_jobs
        {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid actual helper job limits",
            ))
        } else {
            Ok(self)
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessBudgetSnapshot {
    pub limits: ProcessBudgetLimits,
    pub charged_jobs: usize,
}
#[derive(Default)]
pub struct ProcessBudget {
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    metadata: Option<PacketDriverMetadataView>,
    limits: ProcessBudgetLimits,
    jobs: PacketMetadataVec<PacketMetadataArc<Entry>>,
}
struct Entry {
    instance: ComponentInstanceId,
    ordinal: usize,
    completion: ScopeRetirementCompletion,
    #[cfg(feature = "native-packet-origin")]
    owner: Arc<zero_tun::AdoptedTunOwner>,
}
impl Entry {
    fn released(&self) -> bool {
        #[cfg(feature = "native-packet-origin")]
        {
            self.completion.is_drained()
                && self.owner.macos_helper_closed()
                && matches!(
                    self.owner.outcome(),
                    zero_tun::TunAcquisitionOutcome::KnownNoNative
                        | zero_tun::TunAcquisitionOutcome::Closed(_)
                )
        }
        #[cfg(not(feature = "native-packet-origin"))]
        {
            false
        }
    }
}
impl std::fmt::Debug for ProcessBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessBudget")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}
impl ProcessBudget {
    /// RuntimeHost binds its exact existing packet Root before publication.
    /// Explicit independent libraries keep their old default construction.
    pub fn new_for_host(memory: PacketDriverMetadataView) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                limits: Default::default(),
                jobs: Default::default(),
                metadata: Some(memory),
            }),
        })
    }

    pub fn new(limits: ProcessBudgetLimits) -> io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                limits: limits.validate()?,
                jobs: Default::default(),
                metadata: None,
            }),
        }))
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn reap(&self) {
        // Clone one concrete original, never an unpaid temporary table. Native
        // observation and the last paid header drop happen outside this lock.
        let mut index = 0;
        loop {
            let actual = self.lock().jobs.get(index).cloned();
            let Some(actual) = actual else { break };
            if actual.released() {
                let retired = {
                    let mut state = self.lock();
                    if state
                        .jobs
                        .get(index)
                        .is_some_and(|old| PacketMetadataArc::ptr_eq(old, &actual))
                    {
                        Some(state.jobs.remove(index))
                    } else {
                        None
                    }
                };
                drop(retired);
            } else {
                index += 1;
            }
        }
        let retired = {
            let mut state = self.lock();
            state
                .jobs
                .is_empty()
                .then(|| std::mem::take(&mut state.jobs))
        };
        drop(retired); // free even the empty table's capacity before metadata refund
    }
    pub fn snapshot(&self) -> ProcessBudgetSnapshot {
        self.reap();
        let state = self.lock();
        ProcessBudgetSnapshot {
            limits: state.limits,
            charged_jobs: state.jobs.len(),
        }
    }
    pub fn check_install(&self, limits: ProcessBudgetLimits) -> io::Result<()> {
        let limits = limits.validate()?;
        self.reap();
        let state = self.lock();
        if state.limits != limits && !state.jobs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "same helper graph still owes Native/Scope ACK retirement",
            ));
        }
        Ok(())
    }
    pub fn install(&self, limits: ProcessBudgetLimits) -> io::Result<()> {
        let limits = limits.validate()?;
        self.reap();
        let mut state = self.lock();
        if state.limits != limits && !state.jobs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "same helper graph remains charged",
            ));
        }
        state.limits = limits;
        Ok(())
    }
    pub fn poll_released(&self, _cx: &mut Context<'_>) {
        self.reap();
    }
    #[cfg(feature = "native-packet-origin")]
    pub fn reserve_macos_tun_helper(
        &self,
        identity: &crate::contract::component::PacketDeviceIdentity,
        ordinal: usize,
        owner: Arc<zero_tun::AdoptedTunOwner>,
        tasks: &WeakTaskRegistrar,
        memory: &PacketDriverMetadataView,
    ) -> io::Result<()> {
        {
            let mut state = self.lock();
            if state
                .metadata
                .as_ref()
                .is_some_and(|actual| !actual.same_root(memory))
            {
                return Err(io::Error::other("foreign helper metadata Root"));
            }
            if state.metadata.is_none() {
                state.metadata = Some(memory.clone());
            }
            if let Some(old) = state
                .jobs
                .iter()
                .find(|old| old.instance == identity.instance() && old.ordinal == ordinal)
            {
                return if Arc::ptr_eq(&old.owner, &owner)
                    && old.completion.scope_identity().same_scope(identity.scope())
                    && identity.scope().same_scope(&tasks.scope_identity())
                {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "foreign original at helper ordinal",
                    ))
                };
            }
        }
        let instance = identity.instance();
        if !identity.scope().same_scope(&tasks.scope_identity()) {
            return Err(io::Error::other("foreign actual helper Scope"));
        }
        let _notifications = memory.notifications();
        self.reap();
        let completion = tasks.retirement_completion(memory)?;
        let layout = PacketMetadataArc::<Entry>::layout().map_err(io::Error::other)?;
        let credit = memory.reserve(layout).map_err(io::Error::other)?;
        let entry = PacketMetadataArc::from_prepaid(
            Entry {
                instance,
                ordinal,
                owner,
                completion,
            },
            credit,
        )
        .map_err(|(source, _, _)| io::Error::other(source))?;
        let mut state = self.lock();
        if state
            .jobs
            .iter()
            .any(|old| old.instance == instance && old.ordinal == ordinal)
        {
            // Only the exact existing provider may make this operation idempotent.
            if state.jobs.iter().any(|old| {
                old.instance == instance
                    && old.ordinal == ordinal
                    && Arc::ptr_eq(&old.owner, &entry.owner)
            }) {
                return Ok(());
            }
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "foreign helper at actual Source ordinal",
            ));
        }
        if state.jobs.len() >= state.limits.max_jobs
            || state
                .jobs
                .iter()
                .filter(|old| old.instance == instance)
                .count()
                >= state.limits.max_jobs_per_instance
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "actual helper job budget exhausted before native IO",
            ));
        }
        let max = state.limits.max_jobs;
        match state.jobs.try_push_with_metadata(entry, memory, max) {
            Ok(()) => Ok(()),
            Err((source, entry)) => {
                drop(state);
                drop(entry);
                Err(io::Error::other(source))
            }
        }
    }
}

#[cfg(all(test, feature = "native-packet-origin"))]
#[path = "macos_budget/tests.rs"]
mod tests;
