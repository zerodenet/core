//! Non-authorizing observation of the one actual physical carrier owner.
//! No resource grant, session admission, task scope or second socket list lives here.
use super::{grant::CarrierGrant, owner::Shared};
use crate::contract::{scope::TaskJoinWitness, ChannelError};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
};
use zero_core::component::{ComponentInstanceId, TaskId};

#[derive(Default)]
pub(super) struct Completion {
    native_closed: AtomicBool,
    pub(super) users_closed: AtomicBool,
    pub(super) users_unknown: AtomicBool,
    pub(super) acquired: AtomicBool,
    pub(super) caller_failure: std::sync::Mutex<Option<crate::contract::scope::TaskJoined>>,
}
impl Completion {
    pub(super) fn confirm(&self) {
        self.native_closed.store(true, Ordering::Release);
        if !self.users_unknown.load(Ordering::Acquire) {
            self.users_closed.store(true, Ordering::Release);
        }
    }
}
/// Identity/observation only. Possession never grants a native acquisition.
/// A registration cannot retain the owner, original controls, tasks or runtime.
#[derive(Clone)]
pub struct CarrierOwnerRegistration {
    pub(super) owner: Weak<Shared>,
    completion: Arc<Completion>,
    instance: ComponentInstanceId,
    limit: NonZeroUsize,
}
/// Private-origin generation physical closure, not protocol user/task completion.
/// A host must additionally confirm its actual transferred-user/resource graph.
#[derive(Clone)]
pub struct CarrierOwnerClosed {
    completion: Arc<Completion>,
    instance: ComponentInstanceId,
}
/// Diagnostic snapshot, never an admission capability or close receipt.
pub struct CarrierOwnerObservation {
    live: bool,
    stopped: bool,
    slots: Vec<CarrierSlotObservation>,
}
/// Bounded current-slot facts. No historical errors/tasks or user-completion
/// claims: physical_view_live describes the original carrier view, not business users.
pub struct CarrierSlotObservation {
    ordinal: u64,
    opening: bool,
    spawning: bool,
    physical_view_live: bool,
    driver: Option<(TaskId, TaskJoinWitness)>,
    acquisition_unknown: Option<Arc<ChannelError>>,
    native: Option<CarrierNativeCloseObservation>,
}
pub struct CarrierNativeCloseObservation {
    closed: bool,
    first_fault: Option<Arc<std::io::Error>>,
    disposition_unknown: Option<Arc<std::io::Error>>,
    operation_pending: bool,
}
impl CarrierNativeCloseObservation {
    pub fn disposition_unknown(&self) -> Option<&Arc<std::io::Error>> {
        self.disposition_unknown.as_ref()
    }
    pub fn operation_pending(&self) -> bool {
        self.operation_pending
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    pub fn first_fault(&self) -> Option<&Arc<std::io::Error>> {
        self.first_fault.as_ref()
    }
}
impl CarrierSlotObservation {
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }
    pub fn is_opening(&self) -> bool {
        self.opening
    }
    pub fn is_spawning(&self) -> bool {
        self.spawning
    }
    pub fn physical_view_live(&self) -> bool {
        self.physical_view_live
    }
    pub fn driver(&self) -> Option<&(TaskId, TaskJoinWitness)> {
        self.driver.as_ref()
    }
    pub fn acquisition_unknown(&self) -> Option<&Arc<ChannelError>> {
        self.acquisition_unknown.as_ref()
    }
    pub fn native(&self) -> Option<&CarrierNativeCloseObservation> {
        self.native.as_ref()
    }
}
impl CarrierOwnerObservation {
    pub fn slots(&self) -> &[CarrierSlotObservation] {
        &self.slots
    }
    pub fn is_live(&self) -> bool {
        self.live
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }
    pub fn retained(&self) -> usize {
        self.slots.len()
    }
}
impl CarrierOwnerRegistration {
    pub(super) fn users_completion(&self) -> Arc<Completion> {
        self.completion.clone()
    }
    pub(super) fn matches_users_completion(&self, completion: &Arc<Completion>) -> bool {
        Arc::ptr_eq(&self.completion, completion)
    }
    pub(super) fn new(owner: &Arc<Shared>) -> Self {
        Self {
            owner: Arc::downgrade(owner),
            completion: owner.completion.clone(),
            instance: owner.instance,
            limit: owner.limit,
        }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn limit(&self) -> NonZeroUsize {
        self.limit
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.owner, &other.owner)
    }
    pub fn matches_grant(&self, grant: &CarrierGrant) -> bool {
        Weak::ptr_eq(&self.owner, &grant.slot.owner)
    }
    /// Pure identity for a reservation already observed by the real host.
    /// It grants no acquisition, caller binding or completion authority.
    pub fn matches_adoption(&self, adoption: &super::CarrierAdoption) -> bool {
        Weak::ptr_eq(&self.owner, &adoption.slot.owner)
    }
    /// None means unavailable, never implicitly closed. Count zero alone does
    /// not prove that the generation can no longer admit a new original.
    pub fn observe(&self) -> Option<CarrierOwnerObservation> {
        let owner = self.owner.upgrade()?;
        let (live, stopped, slots) = {
            let state = owner.state();
            (state.live, state.stopped, state.slots.clone())
        };
        let slots = slots
            .into_iter()
            .map(|slot| {
                let (mut observation, native) = {
                    let state = slot.state();
                    (
                        CarrierSlotObservation {
                            ordinal: slot.ordinal,
                            opening: state.opening,
                            spawning: state.spawning,
                            physical_view_live: state.view_live,
                            driver: state.driver.clone(),
                            acquisition_unknown: state.unknown.clone(),
                            native: None,
                        },
                        state.close_observer.clone(),
                    )
                };
                // No carrier/scope/control lock surrounds the original TCP getter.
                observation.native = native.and_then(|native| native.observe()).map(|native| {
                    CarrierNativeCloseObservation {
                        closed: native.closed,
                        first_fault: native.first_fault,
                        disposition_unknown: native.disposition_unknown,
                        operation_pending: native.operation_pending,
                    }
                });
                observation
            })
            .collect();
        Some(CarrierOwnerObservation {
            live: live && !owner.cancel.is_cancelled(),
            stopped: stopped || owner.cancel.is_cancelled(),
            slots,
        })
    }
    /// Only the actual Owner's successful explicit shutdown can issue this.
    /// Drop, expired Weak, empty records and TaskJoined never issue a receipt.
    pub fn closed(&self) -> Option<CarrierOwnerClosed> {
        self.completion
            .native_closed
            .load(Ordering::Acquire)
            .then(|| CarrierOwnerClosed {
                completion: self.completion.clone(),
                instance: self.instance,
            })
    }
}
impl CarrierOwnerClosed {
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn matches(&self, registration: &CarrierOwnerRegistration) -> bool {
        Arc::ptr_eq(&self.completion, &registration.completion)
    }
}
