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
mod completion;
mod icmp;
pub use completion::{MetadataPoolRegistrationObservation, MetadataPoolRegistrationReleased};
pub use icmp::{MetadataIcmpAttempt, MetadataIcmpAuthorization, MetadataIcmpMemory};
mod close;
mod permit;
mod preparation;
mod protocol_control;
mod registration;
mod sources;
pub use sources::{MetadataOriginalFault, MetadataOriginalNamespace, MetadataOriginalSources};
mod totals;
pub(crate) use permit::MetadataCandidate;
pub use permit::{MetadataPoolClosed, MetadataPoolOrigin, MetadataPoolPermit};
pub use preparation::MetadataPoolPreparation;
pub use registration::{MetadataPoolOwnerClosed, MetadataPoolRegistration};
pub(crate) use totals::MetadataTotalsOriginal;

// Only sealed current candidates can populate this immutable observation.
pub(crate) type MetadataPoolBinding = (
    zero_core::request::Destination,
    crate::contract::network::UpstreamBinding,
    Option<crate::contract::dns::DnsGeneration>,
);

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
    closed: Arc<OnceLock<bool>>,
    confirmed_native: std::sync::atomic::AtomicBool,
    state: Mutex<State>,
    changed: Arc<tokio::sync::Notify>,
    totals: OnceLock<Arc<crate::controlled_carrier::CarrierOriginalTotals>>,
}
struct State {
    bound: Option<Arc<binding::Authority>>,
    live: bool,
    stopped: bool,
    next: u64,
    slots: BTreeMap<u64, Arc<Slot>>,
    last_binding: Option<MetadataPoolBinding>,
}
struct Slot {
    owner: Weak<Shared>,
    instance: ComponentInstanceId,
    ordinal: u64,
    closed: completion::Completion,
    retired: CancellationToken,
    protocol: Mutex<Option<Arc<crate::original_protocol::Phase>>>,
    protocol_stop: OnceLock<CancellationToken>,
    waker: Mutex<Option<std::task::Waker>>,
    caller: OnceLock<TaskJoinWitness>,
    preparing: std::sync::atomic::AtomicBool,
    registration_uncertain: std::sync::atomic::AtomicBool,
    registration_scope: Option<crate::contract::scope::ScopeRetirementObservation>,
    registration: MetadataPoolRegistration,
    native: Mutex<Native>,
    close_cohort: OnceLock<sources::MetadataCloseCohort>,
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
    binding: Option<MetadataPoolBinding>,
    selection: Option<(
        sources::memory::Allocation<crate::EgressSelection>,
        crate::EgressInterfaceControl,
    )>,
    selection_credit: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    icmp: Option<zero_traits::packet_metadata::PacketMetadataWeak<icmp::Attempt>>,
    icmp_prepared: bool,
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
                confirmed_native: std::sync::atomic::AtomicBool::new(false),
                changed: Arc::default(),
                totals: OnceLock::new(),
                state: Mutex::new(State {
                    bound: None,
                    live: false,
                    stopped: false,
                    next: 0,
                    slots: BTreeMap::new(),
                    last_binding: None,
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
            let _ = self.shared.closed.set(
                self.shared
                    .confirmed_native
                    .load(std::sync::atomic::Ordering::Acquire),
            );
        }
        if !self
            .shared
            .state
            .lock()
            .expect("metadata pool poisoned")
            .slots
            .is_empty()
        {
            report.unfinished.push(Unfinished::Resource(
                "metadata original registration ACK remains retained".into(),
            ));
        }
        failure.map_or(Ok(report), Err)
    }
}
impl Drop for MetadataPoolOwner {
    fn drop(&mut self) {
        self.revoke();
    }
}
mod grant;
