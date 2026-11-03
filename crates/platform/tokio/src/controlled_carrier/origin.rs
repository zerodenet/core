//! Read-only lineage of the actual native carrier and its optional real driver.
use super::{error, slot::Slot, CarrierAdoption, CarrierOwnerRegistration};
use crate::contract::{scope::TaskJoinWitness, ChannelError, DeliveryState, Reason, Stage};
use std::sync::{Arc, Weak};
use zero_core::component::ComponentInstanceId;

/// A sealed shared observation, never native I/O or shutdown authority.
/// Erasure here also keeps recursive Slot/LoanOwner internals out of upstream
/// Send/Sync inference and makes per-session clones one reference-count change.
#[derive(Clone)]
pub struct CarrierOrigin(Arc<Original>);
enum Original {
    Carrier(ActualOriginal),
    Metadata(crate::controlled_datagram::metadata_pool::MetadataPoolOrigin),
    MetadataPool {
        owner: crate::controlled_datagram::metadata_pool::MetadataPoolRegistration,
        binding: Option<crate::controlled_datagram::metadata_pool::MetadataPoolBinding>,
    },
}
mod close;
pub use close::CarrierOriginClosed;
use close::ClosedOriginal;
mod owner;
pub use owner::{CarrierOriginOwner, CarrierOriginOwnerClosed, CarrierOriginUsersClosed};
struct ActualOriginal {
    slot: Weak<Slot>,
    owner: CarrierOwnerRegistration,
    driver: Option<TaskJoinWitness>,
    parent: Option<CarrierOrigin>,
    completion: Arc<std::sync::OnceLock<bool>>,
}
impl CarrierAdoption {
    /// Capture only the driver actually registered by this original slot. A
    /// witness from another slot/scope, or an unused grant, cannot name a donor.
    pub fn origin(&self, driver: &TaskJoinWitness) -> Result<CarrierOrigin, ChannelError> {
        let owner = self
            .slot
            .owner
            .upgrade()
            .ok_or_else(|| self.origin_error())?;
        let valid = {
            let state = self.slot.state();
            !state.closed
                && (state.control.is_some() || state.packet.is_some() || state.loan.is_some())
                && state
                    .driver
                    .as_ref()
                    .is_some_and(|(_, actual)| actual.same_registration(driver))
        };
        if !valid {
            return Err(self.origin_error());
        }
        Ok(CarrierOrigin(Arc::new(Original::Carrier(ActualOriginal {
            completion: self.slot.completion.clone(),
            slot: Arc::downgrade(&self.slot),
            owner: CarrierOwnerRegistration::new(&owner),
            driver: Some(driver.clone()),
            parent: self.slot.state().loan_origin.clone(),
        }))))
    }
    pub(super) fn transferred_origin(&self) -> Result<CarrierOrigin, ChannelError> {
        let owner = self
            .slot
            .owner
            .upgrade()
            .ok_or_else(|| self.origin_error())?;
        let mut state = self.slot.state();
        if state.closed
            || state.spawn_attempted
            || (state.control.is_none() && state.packet.is_none())
            || state.loan.is_some()
            || !state
                .users
                .as_ref()
                .is_some_and(|users| users.transferred())
        {
            return Err(self.origin_error());
        }
        state.transferred_origin = true;
        Ok(CarrierOrigin(Arc::new(Original::Carrier(ActualOriginal {
            slot: Arc::downgrade(&self.slot),
            owner: CarrierOwnerRegistration::new(&owner),
            driver: None,
            parent: None,
            completion: self.slot.completion.clone(),
        }))))
    }
    fn origin_error(&self) -> ChannelError {
        error(
            self.instance(),
            Stage::Prepare,
            Reason::StalePlan,
            DeliveryState::NotSubmitted,
        )
    }
}
impl CarrierOrigin {
    /// Retain the real ancillary original, without inventing a carrier slot,
    /// driver or a second native provider.
    pub fn from_metadata(
        original: crate::controlled_datagram::metadata_pool::MetadataPoolOrigin,
    ) -> Self {
        Self(Arc::new(Original::Metadata(original)))
    }
    pub fn from_metadata_pool(
        actual: crate::controlled_datagram::metadata_pool::MetadataPoolOrigin,
    ) -> Self {
        let binding = if !actual.is_retired() && actual.endpoints().is_some() {
            actual.binding_snapshot()
        } else {
            None
        };
        Self(Arc::new(Original::MetadataPool {
            owner: actual.owner().clone(),
            binding,
        }))
    }
    pub fn metadata(
        &self,
    ) -> Option<&crate::controlled_datagram::metadata_pool::MetadataPoolOrigin> {
        match self.0.as_ref() {
            Original::Metadata(origin) => Some(origin),
            _ => None,
        }
    }
    pub fn closed(&self) -> Option<CarrierOriginClosed> {
        match self.0.as_ref() {
            Original::Carrier(origin) => origin
                .completion
                .get()
                .map(|_| CarrierOriginClosed(ClosedOriginal::Carrier(origin.completion.clone()))),
            Original::Metadata(origin) => origin
                .closed()
                .map(|closed| CarrierOriginClosed(ClosedOriginal::Metadata(closed))),
            Original::MetadataPool {
                owner,
                binding: Some(_),
            } => owner
                .closed()
                .map(|closed| CarrierOriginClosed(ClosedOriginal::MetadataPool(closed))),
            Original::MetadataPool { binding: None, .. } => None,
        }
    }
    pub fn parent(&self) -> Option<&Self> {
        match self.0.as_ref() {
            Original::Carrier(origin) => origin.parent.as_ref(),
            Original::Metadata(_) | Original::MetadataPool { .. } => None,
        }
    }
    pub fn native_origin(&self) -> &Self {
        let mut original = self;
        while let Some(parent) = original.parent() {
            original = parent;
        }
        original
    }
    pub fn endpoints(&self) -> Option<super::CarrierEndpoints> {
        match self.native_origin().0.as_ref() {
            Original::Carrier(origin) => origin.slot.upgrade()?.state().endpoints.clone(),
            Original::MetadataPool {
                owner,
                binding: Some(binding),
            } => {
                let origin = owner.current_original_for(binding)?;
                let (local, peer) = origin.endpoints()?;
                Some(super::CarrierEndpoints::metadata(origin, local, peer))
            }
            Original::MetadataPool { binding: None, .. } => None,
            Original::Metadata(origin) => {
                let (local, peer) = origin.endpoints()?;
                Some(super::CarrierEndpoints::metadata(
                    origin.clone(),
                    local,
                    peer,
                ))
            }
        }
    }
    pub fn selected_binding(
        &self,
    ) -> Option<(
        zero_core::request::Destination,
        crate::contract::network::UpstreamBinding,
    )> {
        match self.0.as_ref() {
            Original::Carrier(origin) => {
                let slot = origin.slot.upgrade()?;
                let state = slot.state();
                Some((state.endpoint.clone()?, state.binding.clone()?))
            }
            Original::Metadata(origin) => origin.selected_binding(),
            Original::MetadataPool {
                binding: Some(binding),
                ..
            } => Some((binding.0.clone(), binding.1.clone())),
            Original::MetadataPool { binding: None, .. } => None,
        }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.owner().instance()
    }
    pub fn owner(&self) -> CarrierOriginOwner<'_> {
        match self.0.as_ref() {
            Original::Carrier(origin) => CarrierOriginOwner::Carrier(&origin.owner),
            Original::Metadata(origin) => CarrierOriginOwner::Metadata(origin.owner()),
            Original::MetadataPool { owner, .. } => CarrierOriginOwner::Metadata(owner),
        }
    }
    /// None is unavailable/taskless observation, never a fabricated join.
    pub fn driver(&self) -> Option<&TaskJoinWitness> {
        match self.0.as_ref() {
            Original::Carrier(origin) => origin.driver.as_ref(),
            Original::Metadata(_) | Original::MetadataPool { .. } => None,
        }
    }
    pub fn same_original(&self, other: &Self) -> bool {
        match (self.0.as_ref(), other.0.as_ref()) {
            (Original::Carrier(a), Original::Carrier(b)) => {
                Weak::ptr_eq(&a.slot, &b.slot)
                    && match (&a.driver, &b.driver) {
                        (Some(a), Some(b)) => a.same_registration(b),
                        (None, None) => true,
                        _ => false,
                    }
                    && a.owner.same_owner(&b.owner)
            }
            (Original::Metadata(a), Original::Metadata(b)) => a.same_original(b),
            (
                Original::MetadataPool {
                    owner: a,
                    binding: aa,
                },
                Original::MetadataPool {
                    owner: b,
                    binding: ba,
                },
            ) => aa == ba && a.same_owner(b),
            _ => false,
        }
    }
    pub fn is_retired(&self) -> bool {
        let mut current = Some(self);
        while let Some(origin) = current {
            let retired = match origin.0.as_ref() {
                Original::Carrier(origin) => origin
                    .slot
                    .upgrade()
                    .is_none_or(|slot| slot.cancel.is_cancelled()),
                Original::Metadata(origin) => origin.is_retired(),
                Original::MetadataPool { owner, binding } => binding
                    .as_ref()
                    .is_none_or(|binding| owner.binding_retired(binding)),
            };
            if retired {
                return true;
            }
            current = origin.parent();
        }
        false
    }
    pub async fn retired(&self) {
        use std::{future::Future, pin::Pin};
        let mut current = Some(self);
        let mut waits: Vec<Pin<Box<dyn Future<Output = ()> + Send + '_>>> = Vec::new();
        while let Some(original) = current {
            match original.0.as_ref() {
                Original::Carrier(origin) => {
                    let Some(slot) = origin.slot.upgrade() else {
                        return;
                    };
                    waits.push(Box::pin(slot.cancel.clone().cancelled_owned()));
                }
                Original::Metadata(origin) => waits.push(Box::pin(origin.retired())),
                Original::MetadataPool {
                    owner,
                    binding: Some(binding),
                } => waits.push(Box::pin(owner.binding_retired_wait(binding))),
                Original::MetadataPool { binding: None, .. } => return,
            }
            current = original.parent();
        }
        std::future::poll_fn(move |cx| {
            for wait in &mut waits {
                if wait.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(());
                }
            }
            std::task::Poll::Pending
        })
        .await;
    }
}
