//! Sole exact service-root originals, installed before native acquisition.
use super::*;
use std::{
    collections::BTreeSet,
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
};
use zero_core::component::{ComponentInstanceId, NetworkGeneration};
use zero_platform_tokio::{
    contract::{
        component::{
            GrantedRootDatagram, RootDatagramAttemptFailure, RootDatagramAuthority,
            RootDatagramClosed, RootDatagramGrant, RootDatagramRequest, RootDatagramUse,
        },
        scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity},
    },
    controlled_datagram::{ControlledDatagramControl, ControlledDatagramOwner},
    EgressInterfaceControl,
};
mod acquire;
mod close;
mod evidence;
mod issue;
pub(in crate::component) use evidence::RootDatagramEvidence;
pub(super) struct RootDeclarations {
    identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
    entries: BTreeMap<usize, Entry>,
    context: Mutex<Option<Arc<RootContext>>>,
    live: AtomicBool,
    closed: AtomicBool,
}
struct RootContext {
    authority: RootDatagramAuthority,
    egress: EgressInterfaceControl,
    ordinals: BTreeSet<usize>,
    max_queries: NonZeroUsize,
    queries: Mutex<Vec<TaskJoinWitness>>,
}
struct Entry {
    peer: SocketAddr,
    generation: NetworkGeneration,
    native: Mutex<ControlledDatagramOwner>,
    records: Mutex<Vec<Arc<Query>>>,
    ledger: Mutex<Ledger>,
}
struct Query {
    witness: TaskJoinWitness,
    attempt: Mutex<Attempt>,
}
struct Attempt {
    opening: bool,
    settled_without_view: bool,
    control: Option<Arc<ControlledDatagramControl>>,
    usage: Option<RootDatagramUse>,
}
#[derive(Default)]
struct Ledger {
    pending_closed: u64,
    total_closed: u64,
    known_no_native: u64,
    first_fault: Option<Arc<ChannelError>>,
    first_query_failure: Option<zero_platform_tokio::contract::scope::TaskJoined>,
    first_close_source: Option<Arc<std::io::Error>>,
}
impl RootDeclarations {
    pub(super) fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
    ) -> Result<Self, ChannelError> {
        let mut entries = BTreeMap::new();
        let mut peers = BTreeSet::new();
        for (ordinal, declaration) in declarations.iter().enumerate() {
            let ResourceDeclaration::ControlledOutboundDatagram {
                peer,
                network_generation,
                max_sockets,
            } = declaration
            else {
                continue;
            };
            if identity.is_none()
                || peer.port() == 0
                || peer.ip().is_unspecified()
                || !peers.insert((*peer, *network_generation))
            {
                return Err(error(
                    component,
                    Reason::InvalidConfig,
                    "root UDP requires actual instance/scope and unique exact nonzero peers",
                ));
            }
            entries.insert(
                ordinal,
                Entry {
                    peer: *peer,
                    generation: *network_generation,
                    native: Mutex::new(ControlledDatagramOwner::new(*max_sockets)),
                    records: Mutex::new(Vec::new()),
                    ledger: Mutex::new(Ledger::default()),
                },
            );
        }
        Ok(Self {
            identity,
            entries,
            context: Mutex::new(None),
            live: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }
    fn context(&self) -> Option<Arc<RootContext>> {
        self.context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    fn admitted(&self, cancel: &CancellationToken) -> bool {
        self.live.load(Ordering::Acquire)
            && !self.closed.load(Ordering::Acquire)
            && !cancel.is_cancelled()
    }
    pub(super) fn activate(&self) {
        self.live.store(true, Ordering::Release);
    }
    fn fault(entry: &Entry, failure: ChannelError) {
        entry
            .ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .first_fault
            .get_or_insert_with(|| Arc::new(failure));
    }
}
impl Entry {
    fn queries(&self) -> Vec<Arc<Query>> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    fn controls(&self) -> Vec<(Arc<ControlledDatagramControl>, Option<RootDatagramUse>)> {
        self.queries()
            .into_iter()
            .filter_map(|query| {
                let state = query
                    .attempt
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state
                    .control
                    .clone()
                    .map(|control| (control, state.usage.clone()))
            })
            .collect()
    }
    fn persist_report(&self, component: ComponentId) {
        let report = match self.native.try_lock() {
            Ok(mut owner) => owner.take_report(),
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner().take_report(),
            Err(std::sync::TryLockError::WouldBlock) => return,
        };
        let mut ledger = self
            .ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ledger.pending_closed += report.confirmed_closed;
        ledger.total_closed += report.confirmed_closed;
        ledger.known_no_native += report.known_no_native;
        if ledger.first_fault.is_none() {
            if let Some(unknown) = report.unconfirmed.first() {
                ledger.first_fault = Some(Arc::new(
                    error(
                        component,
                        Reason::Io,
                        "root original UDP disposition remains unknown",
                    )
                    .with_source(unknown.source.clone()),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests;
