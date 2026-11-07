//! Actual driver task records. This does not own accepted applications, masks
//! or endpoint handles and cannot issue an all-users/native close report.
use crate::contract::{
    scope::{SpawnError, TaskClass, TaskJoinWitness, TaskJoined, TaskRegistrar},
    ChannelError, Impact, Reason, Stage,
};
use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
};
use zero_core::component::TaskId;
mod body;
mod completion;
mod join;
mod reception;
pub(in crate::controlled_datagram::metadata::users) use completion::RegistrationSequence;
pub use completion::{DriverRegistrationsClosed, RegistrationIter};
mod registration;
pub use reception::{DriverDelivery, DriverExitReceiver};

pub trait DriverKind: Copy + fmt::Debug + Send + Sync + 'static {}
impl<T: Copy + fmt::Debug + Send + Sync + 'static> DriverKind for T {}

pub struct DriverOwner<O: DriverKind, B: Send + Sync + 'static> {
    shared: Arc<Shared<O, B>>,
}
struct Shared<O: DriverKind, B: Send + Sync + 'static> {
    registrar: TaskRegistrar,
    limit: NonZeroUsize,
    state: Arc<Mutex<State<O, B>>>,
    changed: tokio::sync::Notify,
}
struct State<O: DriverKind, B: Send + Sync + 'static> {
    pending: usize,
    retired: bool,
    records: BTreeMap<TaskId, Arc<Record<O, B>>>,
    rejected: Option<Arc<DriverRegistrationFailure<O>>>,
    reclaimed: u64,
    count_overflowed: bool,
}
struct Record<O: DriverKind, B: Send + Sync + 'static> {
    origin: O,
    owner: Weak<Mutex<State<O, B>>>,
    registration: Mutex<Option<(TaskId, TaskJoinWitness)>>,
    body: Mutex<body::BodyState<B>>,
    received: Mutex<Option<TaskJoined>>,
    delivered: AtomicBool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriverRegistrationReason {
    LocalCapacity,
    Scope(SpawnError),
    Retired,
    UnsupportedGenericSpawn,
    ObserverFault,
}
#[derive(Debug)]
pub struct DriverRegistrationFailure<O: DriverKind> {
    pub origin: Option<O>,
    pub reason: DriverRegistrationReason,
}
impl<O: DriverKind> fmt::Display for DriverRegistrationFailure<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "driver registration rejected: {:?}", self.reason)
    }
}
impl<O: DriverKind> std::error::Error for DriverRegistrationFailure<O> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.reason {
            DriverRegistrationReason::Scope(source) => Some(source),
            _ => None,
        }
    }
}
/// Bounded task-only status. Body completion and receipt observation do not
/// mark an exit received, release hosted publication credit or close any I/O.
#[derive(Debug)]
pub struct DriverTaskStatus<O: DriverKind> {
    pub pending_registration: usize,
    pub retained: usize,
    pub unjoined: Vec<TaskId>,
    pub unreceived: Vec<TaskId>,
    pub reclaimed: u64,
    pub count_overflowed: bool,
    pub rejection: Option<Arc<DriverRegistrationFailure<O>>>,
}
impl<O: DriverKind, B: Send + Sync + 'static> DriverOwner<O, B> {
    /// Uses only this existing host registrar. No scope or runtime is created.
    pub fn new(
        registrar: TaskRegistrar,
        max_retained_drivers: NonZeroUsize,
    ) -> (Self, DriverExitReceiver<O, B>) {
        let shared = Arc::new(Shared {
            registrar,
            limit: max_retained_drivers,
            changed: tokio::sync::Notify::new(),
            state: Arc::new(Mutex::new(State {
                pending: 0,
                retired: false,
                records: BTreeMap::new(),
                rejected: None,
                reclaimed: 0,
                count_overflowed: false,
            })),
        });
        (
            Self {
                shared: shared.clone(),
            },
            DriverExitReceiver { shared },
        )
    }
    pub fn downgrade(&self) -> WeakDrivers<O, B> {
        WeakDrivers(Arc::downgrade(&self.shared))
    }
    pub fn scope_identity(&self) -> crate::contract::scope::TaskScopeIdentity {
        self.shared.registrar.scope_identity()
    }
    pub fn reject(&self, origin: Option<O>, reason: DriverRegistrationReason) {
        self.shared.reject(origin, reason);
    }
    pub fn revoke_registration(&self) {
        self.shared.state.lock().unwrap().retired = true;
        self.shared.changed.notify_waiters();
    }
    pub fn admission(&self) -> Admission<O, B> {
        Admission(Arc::downgrade(&self.shared))
    }
    pub fn task_status(&self) -> DriverTaskStatus<O> {
        let state = self.shared.state.lock().unwrap();
        let mut unjoined = Vec::new();
        let mut unreceived = Vec::new();
        for (id, record) in &state.records {
            let joined = record
                .registration
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|(_, witness)| witness.try_joined());
            if joined.is_none() {
                unjoined.push(*id);
            }
            if !record.delivered.load(Ordering::Acquire) {
                unreceived.push(*id);
            }
        }
        DriverTaskStatus {
            pending_registration: state.pending,
            retained: state.records.len(),
            unjoined,
            unreceived,
            reclaimed: state.reclaimed,
            count_overflowed: state.count_overflowed,
            rejection: state.rejected.clone(),
        }
    }
    pub fn tasks(&self) -> Vec<TaskId> {
        self.shared
            .state
            .lock()
            .unwrap()
            .records
            .keys()
            .copied()
            .collect()
    }
    /// Original registration metadata, including before the body completes.
    pub fn origin(&self, id: TaskId) -> Option<O> {
        self.shared
            .state
            .lock()
            .unwrap()
            .records
            .get(&id)
            .map(|record| record.origin)
    }
    pub fn body(&self, id: TaskId) -> Option<Arc<B>> {
        let record = self.shared.state.lock().unwrap().records.get(&id)?.clone();
        let body = record.body.lock().unwrap().current();
        body
    }
    /// Reclaim only actual joined and explicitly received records. Hosted
    /// publication credit remains held until the real host acknowledges it.
    pub fn reclaim_received(&self) {
        let candidates: Vec<_> = self
            .shared
            .state
            .lock()
            .unwrap()
            .records
            .iter()
            .map(|(id, record)| (*id, record.clone()))
            .collect();
        let ready: Vec<_> = candidates
            .into_iter()
            .filter(|(_, record)| {
                if !record.delivered.load(Ordering::Acquire) {
                    return false;
                }
                let witness = record
                    .registration
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|(_, witness)| witness.clone());
                witness.is_some_and(|witness| {
                    witness.try_joined().is_some_and(|joined| {
                        witness.matches_joined(&joined) && joined.registration_released()
                    })
                })
            })
            .collect();
        let retired = {
            let mut state = self.shared.state.lock().unwrap();
            let mut retired = Vec::with_capacity(ready.len());
            for (id, original) in ready.iter() {
                if !state
                    .records
                    .get(id)
                    .is_some_and(|actual| Arc::ptr_eq(actual, original))
                {
                    continue;
                }
                retired.push(state.records.remove(id).unwrap());
                match state.reclaimed.checked_add(1) {
                    Some(count) => state.reclaimed = count,
                    None => state.count_overflowed = true,
                }
            }
            retired
        };
        drop(retired); // original sources and witnesses drop outside the owner gate
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> Shared<O, B> {
    fn reject(&self, origin: Option<O>, reason: DriverRegistrationReason) {
        let mut state = self.state.lock().unwrap();
        state.retired = true;
        state
            .rejected
            .get_or_insert_with(|| Arc::new(DriverRegistrationFailure { origin, reason }));
        drop(state);
        self.changed.notify_waiters();
    }
}

mod admission;
pub use admission::{Admission, WeakDrivers};

#[cfg(test)]
mod tests;
