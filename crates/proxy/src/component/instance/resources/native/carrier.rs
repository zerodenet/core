//! Exact ordinal bindings to the protocol's sole physical owner, never a pool.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::scope::{TaskRegistrar, TaskScopeIdentity},
    controlled_carrier::{CarrierOwnerBinding, CarrierOwnerRegistration},
};

mod lookup;
mod register;

pub(super) struct CarrierDeclarations {
    identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
    declarations: BTreeMap<usize, ResourceDeclaration>,
    records: Mutex<BTreeMap<usize, Registration>>,
    host_cancel: Option<Arc<CancellationToken>>,
}
struct Registration {
    actual: CarrierOwnerRegistration,
    bound: Option<CarrierOwnerBinding>,
    first_fault: Option<Arc<ChannelError>>,
    first_native_fault: Option<Arc<std::io::Error>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::component) enum CarrierUsersEvidence {
    Unaccounted,
}
#[derive(Clone, Debug)]
pub(in crate::component) struct CarrierIoEvidence {
    pub ordinal: usize,
    pub users: CarrierUsersEvidence,
    pub registered: bool,
    pub owner_available: bool,
    /// Issued only by the actual owner's successful explicit shutdown.
    pub native_generation_closed: bool,
    pub first_fault: Option<Arc<ChannelError>>,
    pub first_native_fault: Option<Arc<std::io::Error>>,
    pub acquisition_unknown: Option<Arc<ChannelError>>,
    pub native_unknown: Option<Arc<std::io::Error>>,
}
impl CarrierIoEvidence {
    pub(super) fn physical_confirmed(&self) -> bool {
        // No canonical transferred-user witness has been integrated yet. An
        // actual native receipt remains visible but cannot seal this provider.
        if !self.registered || !self.native_generation_closed {
            return false;
        }
        match self.users {
            CarrierUsersEvidence::Unaccounted => false,
        }
    }
}
impl CarrierDeclarations {
    pub(super) fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let declarations: BTreeMap<_, _> = declarations
            .iter()
            .enumerate()
            .filter(|(_, declaration)| {
                matches!(
                    declaration,
                    ResourceDeclaration::ControlledOutboundStream { .. }
                )
            })
            .map(|(ordinal, declaration)| (ordinal, declaration.clone()))
            .collect();
        if !declarations.is_empty() && identity.is_none() {
            return Err(error(
                component,
                Reason::Unsupported,
                "carrier registration requires the actual instance and host scope",
            ));
        }
        let host_cancel = (!declarations.is_empty()).then(|| Arc::new(cancel));
        Ok(Self {
            identity,
            declarations,
            records: Mutex::new(BTreeMap::new()),
            host_cancel,
        })
    }
    pub(super) fn revoke(&self) {
        if let Some(cancel) = &self.host_cancel {
            cancel.cancel();
        }
    }
    pub(super) fn evidence(&self) -> Vec<CarrierIoEvidence> {
        // Native/scope observers can run external wakes. Snapshot identities
        // first, then release every host resource lock before observing them.
        let records: BTreeMap<_, _> = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(ordinal, entry)| {
                (
                    *ordinal,
                    (
                        entry.actual.clone(),
                        entry.bound.is_some(),
                        entry.first_fault.clone(),
                        entry.first_native_fault.clone(),
                    ),
                )
            })
            .collect();
        self.declarations
            .keys()
            .map(|ordinal| {
                let mut proof = CarrierIoEvidence {
                    ordinal: *ordinal,
                    users: CarrierUsersEvidence::Unaccounted,
                    registered: false,
                    owner_available: false,
                    native_generation_closed: false,
                    first_fault: None,
                    first_native_fault: None,
                    acquisition_unknown: None,
                    native_unknown: None,
                };
                if let Some((actual, bound, first_fault, first_native_fault)) = records.get(ordinal)
                {
                    proof.registered = *bound;
                    proof.first_fault = first_fault.clone();
                    proof.first_native_fault = first_native_fault.clone();
                    proof.native_generation_closed = actual
                        .closed()
                        .is_some_and(|receipt| receipt.matches(actual));
                    if let Some(observation) = actual.observe() {
                        proof.owner_available = true;
                        for slot in observation.slots() {
                            if proof.acquisition_unknown.is_none() {
                                proof.acquisition_unknown = slot.acquisition_unknown().cloned();
                            }
                            if let Some(native) = slot.native() {
                                if proof.first_native_fault.is_none() {
                                    proof.first_native_fault = native.first_fault().cloned();
                                }
                                if proof.native_unknown.is_none() {
                                    proof.native_unknown = native.disposition_unknown().cloned();
                                }
                            }
                        }
                    }
                    let mut entries = self
                        .records
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(entry) = entries.get_mut(ordinal) {
                        if entry.first_fault.is_none() {
                            entry.first_fault = proof.acquisition_unknown.clone();
                        }
                        if entry.first_native_fault.is_none() {
                            entry.first_native_fault = proof
                                .first_native_fault
                                .clone()
                                .or_else(|| proof.native_unknown.clone());
                        }
                        if entry.first_fault.is_none() {
                            if let Some(source) = &entry.first_native_fault {
                                let mut failure = ChannelError::new(
                                    zero_platform_tokio::contract::Stage::Close,
                                    Reason::Io,
                                    zero_platform_tokio::contract::Impact::Component,
                                )
                                .with_source(NativeSource(source.clone()));
                                if proof.native_unknown.is_some() {
                                    failure.delivery =
                                        zero_platform_tokio::contract::DeliveryState::Unknown;
                                }
                                if let Some((instance, _)) = &self.identity {
                                    failure.component = Some(instance.component);
                                }
                                entry.first_fault = Some(Arc::new(failure));
                            }
                        }
                        proof.first_fault = entry.first_fault.clone();
                    }
                }
                proof
            })
            .collect()
    }
}

#[derive(Debug)]
struct NativeSource(Arc<std::io::Error>);
impl std::fmt::Display for NativeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for NativeSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(test)]
#[path = "../../../tests/resource_carrier.rs"]
mod tests;
