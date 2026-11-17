//! Original TUN helper graphs in the same ProcessBudget as native plugins.
use super::*;
use crate::contract::scope::{ScopeRetirementCompletion, WeakTaskRegistrar};
use zero_tun::packet_memory::{PacketDriverMetadataView, PacketMetadataArc, PacketMetadataVec};
#[derive(Default)]
pub(super) struct Helpers {
    pub(super) metadata: Option<PacketDriverMetadataView>,
    pub(super) jobs: PacketMetadataVec<PacketMetadataArc<Entry>>,
}
pub(super) struct Entry {
    pub(super) instance: ComponentInstanceId,
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
impl ProcessBudget {
    /// Bind the original Host metadata Root; native plugins and TUN helpers
    /// share one finite job book and the same replacement exclusion.
    pub fn new_for_host(memory: PacketDriverMetadataView) -> Arc<Self> {
        let root = Arc::new(Self::default());
        root.lock().helpers.metadata = Some(memory);
        root
    }
    pub(super) fn reap_helpers(&self) {
        // Clone one concrete original, never an unpaid temporary table. Native
        // observation and the last paid header drop happen outside this lock.
        let mut index = 0;
        loop {
            let actual = self.lock().helpers.jobs.get(index).cloned();
            let Some(actual) = actual else { break };
            if actual.released() {
                let retired = {
                    let mut state = self.lock();
                    if state
                        .helpers
                        .jobs
                        .get(index)
                        .is_some_and(|old| PacketMetadataArc::ptr_eq(old, &actual))
                    {
                        Some(state.helpers.jobs.remove(index))
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
                .helpers
                .jobs
                .is_empty()
                .then(|| std::mem::take(&mut state.helpers.jobs))
        };
        drop(retired); // free even the empty table's capacity before metadata refund
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
                .helpers
                .metadata
                .as_ref()
                .is_some_and(|actual| !actual.same_root(memory))
            {
                return Err(io::Error::other("foreign helper metadata Root"));
            }
            if state.helpers.metadata.is_none() {
                state.helpers.metadata = Some(memory.clone());
            }
            if let Some(old) = state
                .helpers
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
            .helpers
            .jobs
            .iter()
            .any(|old| old.instance == instance && old.ordinal == ordinal)
        {
            // Only the exact existing provider may make this operation idempotent.
            if state.helpers.jobs.iter().any(|old| {
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
        if state.total_jobs() >= state.limits.max_jobs
            || state.jobs_for(instance) >= state.limits.max_jobs_per_instance
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "actual helper job budget exhausted before native IO",
            ));
        }
        let max = state.limits.max_jobs;
        match state
            .helpers
            .jobs
            .try_push_with_metadata(entry, memory, max)
        {
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
#[path = "../macos_budget/tests.rs"]
mod tests;
