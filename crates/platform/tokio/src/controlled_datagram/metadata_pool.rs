//! On-demand ancillary originals. Private close stamps include the canonical
//! user/driver graph; Drop, released capacity and native port reuse do not.
use super::metadata::{
    MetadataDatagramControl, MetadataOwner, MetadataOwnerReport, MetadataViewIssuer,
};
use crate::contract::{
    component::{GrantedMetadataListener, ResourceDeclaration},
    lifecycle::{CloseReport, Unfinished},
    scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity},
    CancellationToken, ChannelError, DeliveryState, Impact, Reason, Stage,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock, Weak},
};
use zero_core::component::ComponentInstanceId;
mod binding;
mod permit;
mod registration;
pub(crate) use permit::MetadataCandidate;
pub use permit::{MetadataPoolClosed, MetadataPoolOrigin, MetadataPoolPermit};
pub use registration::{MetadataPoolOwnerClosed, MetadataPoolRegistration};

pub struct MetadataPoolOwner {
    shared: Arc<Shared>,
}
#[derive(Clone)]
pub struct MetadataPoolGrants {
    owner: Weak<Shared>,
}
struct Shared {
    instance: ComponentInstanceId,
    limit: NonZeroUsize,
    limits: super::metadata::MetadataLimits,
    cancel: CancellationToken,
    closed: Arc<OnceLock<()>>,
    state: Mutex<State>,
}
struct State {
    bound: Option<Arc<binding::Authority>>,
    live: bool,
    stopped: bool,
    next: u64,
    slots: BTreeMap<u64, Arc<Slot>>,
}
struct Slot {
    owner: Weak<Shared>,
    instance: ComponentInstanceId,
    ordinal: u64,
    closed: Arc<OnceLock<bool>>,
    retired: CancellationToken,
    caller: TaskJoinWitness,
    registration: MetadataPoolRegistration,
    native: Mutex<Native>,
}
impl Slot {
    fn native(&self) -> Result<std::sync::MutexGuard<'_, Native>, ChannelError> {
        match self.native.try_lock() {
            Ok(guard) => Ok(guard),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Ok(poisoned.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => {
                Err(error(Reason::Capacity, "metadata original owner is busy"))
            }
        }
    }
}
struct Native {
    owner: MetadataOwner,
    control: Option<MetadataDatagramControl>,
    issuer: Option<MetadataViewIssuer>,
    transferred: bool,
    confirmed: Option<super::metadata::users::MetadataUsersClosedReceipt>,
    unbound: Option<(super::metadata::MetadataViewClaim, TaskRegistrar)>,
    acquired_confirmed: bool,
    attempted: bool,
    endpoints: Option<(std::net::SocketAddr, std::net::SocketAddr)>,
    binding: Option<(
        zero_core::request::Destination,
        crate::contract::network::UpstreamBinding,
    )>,
    report: Option<Arc<MetadataOwnerReport>>,
}
fn error(reason: Reason, message: &'static str) -> ChannelError {
    let mut result = ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(std::io::Error::other(message));
    result.delivery = DeliveryState::NotSubmitted;
    result
}
impl MetadataPoolOwner {
    pub fn new(
        instance: ComponentInstanceId,
        limit: NonZeroUsize,
        limits: super::metadata::MetadataLimits,
        parent: &CancellationToken,
    ) -> Result<Self, ChannelError> {
        let _ = MetadataOwner::new(limits).map_err(|source| {
            error(Reason::InvalidConfig, "invalid metadata limits").with_source(source)
        })?;
        Ok(Self {
            shared: Arc::new(Shared {
                instance,
                limit,
                limits,
                cancel: parent.child_token(),
                closed: Arc::default(),
                state: Mutex::new(State {
                    bound: None,
                    live: false,
                    stopped: false,
                    next: 0,
                    slots: BTreeMap::new(),
                }),
            }),
        })
    }
    pub fn registration(&self) -> MetadataPoolRegistration {
        MetadataPoolRegistration::new(&self.shared)
    }
    pub fn grants(&self) -> MetadataPoolGrants {
        MetadataPoolGrants {
            owner: Arc::downgrade(&self.shared),
        }
    }
    pub fn start(&self) -> Result<(), ChannelError> {
        let mut state = self.shared.state.lock().expect("metadata pool poisoned");
        if state.stopped || self.shared.cancel.is_cancelled() || state.bound.is_none() {
            return Err(error(
                Reason::Revoked,
                "metadata pool has no actual host binding",
            ));
        }
        state.live = true;
        Ok(())
    }
    pub fn revoke(&self) {
        self.shared.cancel.cancel();
        self.shared
            .state
            .lock()
            .expect("metadata pool poisoned")
            .stopped = true;
    }
    /// Query/endpoint owners deliver their actual graph receipts first. This
    /// final pass never manufactures users confirmation for retained originals.
    pub fn shutdown(&self) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let slots = self
            .shared
            .state
            .lock()
            .expect("metadata pool poisoned")
            .slots
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut report = CloseReport::default();
        let mut failure = None;
        for slot in slots {
            let permit = MetadataPoolPermit(slot);
            match permit.close_stored() {
                Ok(Some(_)) => {}
                Ok(None) => report.unfinished.push(Unfinished::Resource(
                    "metadata original users/native close remains unconfirmed".into(),
                )),
                Err(source) => {
                    failure.get_or_insert(source);
                }
            }
        }
        if self
            .shared
            .state
            .lock()
            .expect("metadata pool poisoned")
            .slots
            .is_empty()
        {
            let _ = self.shared.closed.set(());
        }
        failure.map_or(Ok(report), Err)
    }
}
impl Drop for MetadataPoolOwner {
    fn drop(&mut self) {
        self.revoke();
    }
}
impl MetadataPoolGrants {
    pub fn retained(&self) -> Option<usize> {
        let owner = self.owner.upgrade()?;
        let count = owner.state.lock().ok()?.slots.len();
        Some(count)
    }
    pub fn reserve(&self, query: &TaskJoinWitness) -> Result<MetadataPoolPermit, ChannelError> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata owner dropped"))?;
        let authority = owner
            .state
            .lock()
            .expect("metadata pool poisoned")
            .bound
            .clone()
            .ok_or_else(|| error(Reason::InvalidConfig, "unregistered metadata pool"))?;
        authority.check(&owner, query)?;
        let native = MetadataOwner::new(owner.limits).map_err(|source| {
            error(Reason::InvalidConfig, "invalid metadata limits").with_source(source)
        })?;
        let mut state = owner.state.lock().expect("metadata pool poisoned");
        if !state.live || state.stopped || owner.cancel.is_cancelled() {
            return Err(error(Reason::Revoked, "metadata admission closed"));
        }
        if state.slots.len() >= owner.limit.get() {
            return Err(error(
                Reason::Capacity,
                "metadata original capacity exhausted",
            ));
        }
        let ordinal = state
            .next
            .checked_add(1)
            .ok_or_else(|| error(Reason::Capacity, "metadata identity exhausted"))?;
        state.next = ordinal;
        let slot = Arc::new(Slot {
            owner: Arc::downgrade(&owner),
            instance: owner.instance,
            ordinal,
            closed: Arc::default(),
            retired: owner.cancel.child_token(),
            caller: query.clone(),
            registration: MetadataPoolRegistration::new(&owner),
            native: Mutex::new(Native {
                owner: native,
                control: None,
                issuer: None,
                transferred: false,
                confirmed: None,
                unbound: None,
                acquired_confirmed: false,
                attempted: false,
                endpoints: None,
                binding: None,
                report: None,
            }),
        });
        state.slots.insert(ordinal, slot.clone());
        Ok(MetadataPoolPermit(slot))
    }
}
