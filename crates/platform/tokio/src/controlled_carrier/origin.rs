//! Read-only lineage of the actual native carrier and its optional real driver.
use super::{error, slot::Slot, CarrierAdoption, CarrierOwnerRegistration};
use crate::contract::{scope::TaskJoinWitness, ChannelError, DeliveryState, Reason, Stage};
use std::sync::{Arc, Weak};
use zero_core::component::ComponentInstanceId;

/// A sealed shared observation, never native I/O or shutdown authority.
/// Erasure here also keeps recursive Slot/LoanOwner internals out of upstream
/// Send/Sync inference and makes per-session clones one reference-count change.
#[derive(Clone)]
pub struct CarrierOrigin(Arc<dyn OriginalObservation>);
// This sealed read-only boundary does not expose or reconstruct a controller.
// Keep the exact finite variant value; only recursive auto-trait inference is erased.
trait OriginalObservation: Send + Sync {
    fn original(&self) -> &Original;
}
impl OriginalObservation for Original {
    fn original(&self) -> &Original {
        self
    }
}
enum Original {
    Carrier(ActualOriginal),
    Packet(PacketProviderRegistration),
    LogicalPacket(LogicalPacketProviderRegistration),
    Listener(crate::controlled_datagram::listener::origin::ListenerCarrierOrigin),
    Metadata(crate::controlled_datagram::metadata_pool::MetadataPoolOrigin),
    MetadataPool {
        owner: crate::controlled_datagram::metadata_pool::MetadataPoolRegistration,
        binding: Option<crate::controlled_datagram::metadata_pool::MetadataPoolBinding>,
    },
}
mod logical_packet;
pub use logical_packet::LogicalPacketProviderRegistration;
mod packet;
pub use packet::{PacketProviderClosed, PacketProviderRegistration};
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
        self.transferred_origin_inner(true)
    }
    pub(super) fn preparing_transferred_origin(&self) -> Result<CarrierOrigin, ChannelError> {
        self.transferred_origin_inner(false)
    }
    fn transferred_origin_inner(&self, publish: bool) -> Result<CarrierOrigin, ChannelError> {
        let owner = self
            .slot
            .owner
            .upgrade()
            .ok_or_else(|| self.origin_error())?;
        let mut state = self.slot.state();
        if state.closed
            || state.spawn_attempted
            || (state.control.is_none() && state.packet.is_none() && state.loan.is_none())
            || (state.loan.is_some() && state.loan_origin.is_none())
            || !state
                .users
                .as_ref()
                .is_some_and(|users| users.transferred())
        {
            return Err(self.origin_error());
        }
        if publish {
            state.transferred_origin = true;
        }
        Ok(CarrierOrigin(Arc::new(Original::Carrier(ActualOriginal {
            slot: Arc::downgrade(&self.slot),
            owner: CarrierOwnerRegistration::new(&owner),
            driver: None,
            parent: state.loan_origin.clone(),
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
    fn observation(&self) -> &Original {
        self.0.as_ref().original()
    }
    /// The actual Source admission fence only. It is not the opaque outer
    /// Packet wire ancestor and never authorizes native-close accounting.
    pub fn packet_source_admission(
        &self,
    ) -> Option<&crate::controlled_datagram::provider::NativeDatagramOrigin> {
        match self.observation() {
            Original::Listener(original) => Some(original),
            Original::LogicalPacket(original) => original.source_admission(),
            _ => None,
        }
    }
    /// New logical opens against the exact actual Source only. It does not
    /// fence already transferred IO or mint a native/Scope/user-close fact.
    pub fn try_enter_packet_source(
        &self,
    ) -> Result<crate::contract::packet::PacketSourceAcquisition, ChannelError> {
        match self.native_origin().observation() {
            Original::Listener(original) => original.try_enter_packet_source(),
            Original::LogicalPacket(original) => original
                .source_admission()
                .ok_or_else(|| {
                    error(
                        self.instance(),
                        Stage::Prepare,
                        Reason::StalePlan,
                        DeliveryState::NotSubmitted,
                    )
                })?
                .try_enter_packet_source(),
            _ => Err(crate::contract::ChannelError::new(
                Stage::Prepare,
                Reason::Unsupported,
                crate::contract::Impact::Component,
            )),
        }
    }

    pub fn from_logical_packet(original: LogicalPacketProviderRegistration) -> Self {
        Self(Arc::new(Original::LogicalPacket(original)))
    }
    pub fn logical_packet(&self) -> Option<&LogicalPacketProviderRegistration> {
        match self.observation() {
            Original::LogicalPacket(actual) => Some(actual),
            _ => None,
        }
    }

    pub fn from_packet(original: PacketProviderRegistration) -> Self {
        Self(Arc::new(Original::Packet(original)))
    }
    pub fn packet(&self) -> Option<&PacketProviderRegistration> {
        match self.observation() {
            Original::Packet(actual) => Some(actual),
            _ => None,
        }
    }

    /// The argument is sealed by the actual listener's unique native controller.
    pub fn from_listener(
        original: crate::controlled_datagram::listener::origin::ListenerCarrierOrigin,
    ) -> Self {
        Self(Arc::new(Original::Listener(original)))
    }
    pub fn listener(
        &self,
    ) -> Option<&crate::controlled_datagram::listener::origin::ListenerCarrierOrigin> {
        match self.observation() {
            Original::Listener(actual) => Some(actual),
            _ => None,
        }
    }
    pub(super) fn names_adoption(&self, adoption: &CarrierAdoption) -> bool {
        matches!(self.observation(), Original::Carrier(origin)
            if Weak::ptr_eq(&origin.slot, &Arc::downgrade(&adoption.slot)))
    }

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
        match self.observation() {
            Original::Metadata(origin) => Some(origin),
            _ => None,
        }
    }
    pub fn closed(&self) -> Option<CarrierOriginClosed> {
        match self.observation() {
            // Scope/driver retirement never certifies the retained outer native.
            Original::LogicalPacket(_) => None,
            Original::Packet(origin) => origin
                .closed()
                .map(|closed| CarrierOriginClosed(ClosedOriginal::Packet(closed))),
            Original::Listener(origin) => origin
                .closed()
                .map(|closed| CarrierOriginClosed(ClosedOriginal::Listener(closed))),
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
        match self.observation() {
            Original::Carrier(origin) => origin.parent.as_ref(),
            Original::Packet(_)
            | Original::LogicalPacket(_)
            | Original::Listener(_)
            | Original::Metadata(_)
            | Original::MetadataPool { .. } => None,
        }
    }
    pub(super) fn related_adoption(&self) -> Option<CarrierAdoption> {
        let Original::Carrier(origin) = self.observation() else {
            return None;
        };
        let slot = origin.slot.upgrade()?;
        let parent = slot.state().related_parent.as_ref()?.upgrade()?;
        let binding = slot.state().binding.clone()?;
        let parent = CarrierAdoption { slot: parent };
        super::related::check_parent(&parent, slot.instance, &binding).ok()?;
        Some(parent)
    }
    /// A physical receiver in this lineage must still store its actual loan
    /// owner and that owner's sealed, strongly retained control cleanup token.
    pub(super) fn retains_related_control(&self, pending: &CarrierAdoption) -> bool {
        let Original::Carrier(origin) = self.observation() else {
            return false;
        };
        let Some(slot) = origin.slot.upgrade() else {
            return false;
        };
        let state = slot.state();
        !state.closed
            && state.loan.is_some()
            && state
                .loan_origin
                .as_ref()
                .zip(origin.parent.as_ref())
                .is_some_and(|(current, captured)| current.same_original(captured))
            && state
                .loan_related
                .as_ref()
                .and_then(Weak::upgrade)
                .is_some_and(|actual| actual.names(pending))
    }
    /// Actual negotiated-control dependency, separate from physical
    /// encapsulation ancestry. This observation grants no cleanup or native I/O.
    pub fn related_control(&self) -> Option<Self> {
        let Original::Carrier(origin) = self.observation() else {
            return None;
        };
        let slot = origin.slot.upgrade()?;
        let parent = slot.state().related_parent.as_ref()?.upgrade()?;
        let owner = parent.owner.upgrade()?;
        let state = parent.state();
        if state.closed
            || parent.cancel.is_cancelled()
            || (state.control.is_none() && state.loan.is_none())
        {
            return None;
        }
        Some(Self(Arc::new(Original::Carrier(ActualOriginal {
            slot: Arc::downgrade(&parent),
            owner: CarrierOwnerRegistration::new(&owner),
            driver: state.driver.as_ref().map(|(_, witness)| witness.clone()),
            parent: state.loan_origin.clone(),
            completion: parent.completion.clone(),
        }))))
    }
    pub fn native_origin(&self) -> &Self {
        let mut original = self;
        while let Some(parent) = original.parent() {
            original = parent;
        }
        original
    }
    pub fn endpoints(&self) -> Option<super::CarrierEndpoints> {
        match self.native_origin().observation() {
            // An unconnected listener has no fixed native peer.
            Original::Packet(_) | Original::LogicalPacket(_) | Original::Listener(_) => None,
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
        match self.observation() {
            Original::Packet(_) | Original::LogicalPacket(_) | Original::Listener(_) => None,
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
        match self.observation() {
            Original::Packet(origin) => CarrierOriginOwner::Packet(origin),
            Original::LogicalPacket(origin) => CarrierOriginOwner::LogicalPacket(origin),
            Original::Listener(origin) => CarrierOriginOwner::Listener(origin.owner()),
            Original::Carrier(origin) => CarrierOriginOwner::Carrier(&origin.owner),
            Original::Metadata(origin) => CarrierOriginOwner::Metadata(origin.owner()),
            Original::MetadataPool { owner, .. } => CarrierOriginOwner::Metadata(owner),
        }
    }
    /// None is unavailable/taskless observation, never a fabricated join.
    pub fn driver(&self) -> Option<&TaskJoinWitness> {
        match self.observation() {
            Original::Packet(origin) => Some(origin.driver()),
            Original::LogicalPacket(origin) => Some(origin.driver()),
            Original::Listener(origin) => Some(origin.driver()),
            Original::Carrier(origin) => origin.driver.as_ref(),
            Original::Metadata(_) | Original::MetadataPool { .. } => None,
        }
    }
    pub fn same_original(&self, other: &Self) -> bool {
        match (self.observation(), other.observation()) {
            (Original::Packet(a), Original::Packet(b)) => a.same_owner(b),
            (Original::LogicalPacket(a), Original::LogicalPacket(b)) => a.same_owner(b),
            (Original::Listener(a), Original::Listener(b)) => a.same_original(b),
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
            let retired = match origin.observation() {
                Original::Packet(origin) => origin.is_retired(),
                Original::LogicalPacket(origin) => origin.is_retired(),
                Original::Listener(origin) => origin.is_retired(),
                Original::Carrier(origin) => origin.slot.upgrade().is_none_or(|slot| {
                    slot.cancel.is_cancelled()
                        || slot
                            .related_cancellation()
                            .is_some_and(|parent| parent.is_cancelled())
                }),
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
            match original.observation() {
                Original::Packet(origin) => waits.push(Box::pin(origin.retired())),
                Original::LogicalPacket(origin) => waits.push(Box::pin(origin.retired())),
                Original::Listener(origin) => waits.push(Box::pin(origin.retired())),
                Original::Carrier(origin) => {
                    let Some(slot) = origin.slot.upgrade() else {
                        return;
                    };
                    waits.push(Box::pin(slot.cancel.clone().cancelled_owned()));
                    if let Some(parent) = slot.related_cancellation() {
                        waits.push(Box::pin(parent.cancelled_owned()));
                    }
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
