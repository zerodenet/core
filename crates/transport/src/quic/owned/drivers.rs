//! Actual driver task records. This does not own accepted applications, masks
//! or endpoint handles and cannot issue an all-users/native close report.
use quinn::{DriverExit, DriverFuture, DriverOrigin};
use std::{
    collections::BTreeMap,
    fmt, io,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
};
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{
    scope::{SpawnError, TaskClass, TaskJoinWitness, TaskJoined, TaskRegistrar},
    ChannelError, Impact, Reason, Stage,
};
mod reception;
mod runtime;
pub use reception::{DriverDelivery, DriverExitReceiver};

pub struct QuinnDriverOwner {
    shared: Arc<Shared>,
}
struct Shared {
    registrar: TaskRegistrar,
    limit: NonZeroUsize,
    state: Mutex<State>,
    changed: tokio::sync::Notify,
}
struct State {
    pending: usize,
    retired: bool,
    records: BTreeMap<TaskId, Arc<Record>>,
    rejected: Option<Arc<DriverRegistrationFailure>>,
    reclaimed: u64,
    count_overflowed: bool,
}
struct Record {
    origin: DriverOrigin,
    registration: Mutex<Option<(TaskId, TaskJoinWitness)>>,
    body: Mutex<Option<Arc<DriverBody>>>,
    received: Mutex<Option<TaskJoined>>,
    delivered: AtomicBool,
}
/// Original driver output, independent of real native task consumption.
#[derive(Debug)]
pub struct DriverBody {
    pub origin: DriverOrigin,
    pub io_error: Option<Arc<io::Error>>,
    pub close_reason: Option<quinn::ConnectionError>,
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
pub struct DriverRegistrationFailure {
    pub origin: Option<DriverOrigin>,
    pub reason: DriverRegistrationReason,
}
impl fmt::Display for DriverRegistrationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Quinn driver registration rejected: {:?}", self.reason)
    }
}
impl std::error::Error for DriverRegistrationFailure {
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
pub struct DriverTaskStatus {
    pub pending_registration: usize,
    pub retained: usize,
    pub unjoined: Vec<TaskId>,
    pub unreceived: Vec<TaskId>,
    pub reclaimed: u64,
    pub count_overflowed: bool,
    pub rejection: Option<Arc<DriverRegistrationFailure>>,
}
impl QuinnDriverOwner {
    /// Uses only this existing host registrar. No scope or runtime is created.
    pub fn new(
        registrar: TaskRegistrar,
        max_retained_drivers: NonZeroUsize,
    ) -> (Self, DriverExitReceiver) {
        let shared = Arc::new(Shared {
            registrar,
            limit: max_retained_drivers,
            changed: tokio::sync::Notify::new(),
            state: Mutex::new(State {
                pending: 0,
                retired: false,
                records: BTreeMap::new(),
                rejected: None,
                reclaimed: 0,
                count_overflowed: false,
            }),
        });
        (
            Self {
                shared: shared.clone(),
            },
            DriverExitReceiver { shared },
        )
    }
    /// Only abstract-socket constructors are supported. This runtime grants no
    /// raw UDP acquisition and supplies no canonical host resource permission.
    pub fn runtime(&self, socket: &super::MetadataSocketOwner) -> Arc<dyn quinn::Runtime> {
        Arc::new(runtime::OwnedRuntime {
            owner: Arc::downgrade(&self.shared),
            callbacks: socket.callbacks.clone(),
        })
    }
    pub fn revoke_registration(&self) {
        self.shared.state.lock().unwrap().retired = true;
        self.shared.changed.notify_waiters();
    }
    pub(super) fn admission(&self) -> Admission {
        Admission(Arc::downgrade(&self.shared))
    }
    pub(super) async fn wait_joined(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<TaskJoined>, ChannelError> {
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (pending, witnesses) = {
                let state = self.shared.state.lock().unwrap();
                (
                    state.pending,
                    state
                        .records
                        .values()
                        .map(|record| {
                            record
                                .registration
                                .lock()
                                .unwrap()
                                .as_ref()
                                .unwrap()
                                .1
                                .clone()
                        })
                        .collect::<Vec<_>>(),
                )
            };
            let mut joined = Vec::with_capacity(witnesses.len());
            for witness in witnesses {
                joined.push(
                    tokio::time::timeout_at(deadline, witness.wait())
                        .await
                        .map_err(|source| {
                            ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                                .with_source(source)
                        })?,
                );
            }
            if pending == 0 {
                return Ok(joined);
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|source| {
                    ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                        .with_source(source)
                })?;
        }
    }
    pub fn task_status(&self) -> DriverTaskStatus {
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
    pub fn body(&self, id: TaskId) -> Option<Arc<DriverBody>> {
        let record = self.shared.state.lock().unwrap().records.get(&id)?.clone();
        let body = record.body.lock().unwrap().clone();
        body
    }
    /// Reclaim only actual joined and explicitly received records. Hosted
    /// publication credit remains held until the real host acknowledges it.
    pub fn reclaim_received(&self) {
        let retired = {
            let mut state = self.shared.state.lock().unwrap();
            let ids: Vec<_> = state
                .records
                .iter()
                .filter_map(|(id, record)| record.delivered.load(Ordering::Acquire).then_some(*id))
                .collect();
            let mut retired = Vec::with_capacity(ids.len());
            for id in ids {
                retired.push(state.records.remove(&id).unwrap());
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
impl Shared {
    fn reject(&self, origin: Option<DriverOrigin>, reason: DriverRegistrationReason) {
        let mut state = self.state.lock().unwrap();
        state.retired = true;
        state
            .rejected
            .get_or_insert_with(|| Arc::new(DriverRegistrationFailure { origin, reason }));
        drop(state);
        self.changed.notify_waiters();
    }
}

#[derive(Clone)]
pub(super) struct Admission(Weak<Shared>);
impl Admission {
    pub(super) fn check(&self) -> Result<(), ChannelError> {
        let owner = self
            .0
            .upgrade()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?;
        let state = owner.state.lock().unwrap();
        if let Some(source) = &state.rejected {
            let reason = match source.reason {
                DriverRegistrationReason::LocalCapacity
                | DriverRegistrationReason::Scope(SpawnError::Capacity) => Reason::Capacity,
                DriverRegistrationReason::Retired
                | DriverRegistrationReason::Scope(SpawnError::Closing) => Reason::Revoked,
                _ => Reason::Internal,
            };
            return Err(ChannelError::new(Stage::Prepare, reason, Impact::Component)
                .with_source(SharedRejection(source.clone())));
        }
        if state.retired {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        Ok(())
    }
}
#[derive(Debug)]
struct SharedRejection(Arc<DriverRegistrationFailure>);
impl fmt::Display for SharedRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedRejection {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
