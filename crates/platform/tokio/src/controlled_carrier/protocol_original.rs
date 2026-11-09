//! Sealed pre-delivery observation of one Root-held protocol original.
//! No allocator, IO, driver/Scope grant, close receipt or alternate cleanup.
use super::*;
use crate::contract::{
    network::{PendingCarrierObserver, UpstreamBinding},
    scope::TaskScopeIdentity,
};
use crate::controlled_datagram::metadata_pool::MetadataPoolOrigin;
use std::sync::Arc;
#[derive(Clone)]
pub struct ProtocolOriginal {
    original: Original,
    scope: TaskScopeIdentity,
    binding: Arc<UpstreamBinding>,
    observer: PendingCarrierObserver,
    retirement: CarrierProtocolRetirement,
}
#[derive(Clone)]
enum Original {
    Metadata(MetadataPoolOrigin),
    Carrier(CarrierAdoption),
}
impl ProtocolOriginal {
    pub(crate) fn metadata_original(
        original: MetadataPoolOrigin,
        scope: TaskScopeIdentity,
        binding: Arc<UpstreamBinding>,
        observer: PendingCarrierObserver,
        retirement: CarrierProtocolRetirement,
    ) -> Self {
        Self {
            original: Original::Metadata(original),
            scope,
            binding,
            observer,
            retirement,
        }
    }
    pub(crate) fn carrier_original(
        original: CarrierAdoption,
        scope: TaskScopeIdentity,
        binding: Arc<UpstreamBinding>,
        observer: PendingCarrierObserver,
        retirement: CarrierProtocolRetirement,
    ) -> Self {
        Self {
            original: Original::Carrier(original),
            scope,
            binding,
            observer,
            retirement,
        }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        match &self.original {
            Original::Metadata(original) => original.instance(),
            Original::Carrier(original) => original.instance(),
        }
    }
    pub fn scope_identity(&self) -> &TaskScopeIdentity {
        &self.scope
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.binding
    }
    pub fn metadata(&self) -> Option<&MetadataPoolOrigin> {
        match &self.original {
            Original::Metadata(original) => Some(original),
            _ => None,
        }
    }
    pub fn adoption(&self) -> Option<&CarrierAdoption> {
        match &self.original {
            Original::Carrier(original) => Some(original),
            _ => None,
        }
    }
    pub fn same_original(&self, other: &Self) -> bool {
        match (&self.original, &other.original) {
            (Original::Metadata(a), Original::Metadata(b)) => a.same_original(b),
            (Original::Carrier(a), Original::Carrier(b)) => a.same_slot(b),
            _ => false,
        }
    }
    pub fn matches_observer(&self, observer: &PendingCarrierObserver) -> bool {
        self.observer.same_observer(observer)
    }
    /// Only the actual Root's original removal writes this flag, after native,
    /// logical users, original outcome/fault reception and every real Host ACK.
    pub fn is_settled(&self) -> bool {
        self.retirement.is_settled()
    }
    pub fn request_retirement(&self) {
        self.retirement.request_pre_delivery_retirement();
    }
    /// A separate actual cached receiver handoff never changes global settled.
    pub fn receiver(&self) -> Option<ProtocolOriginalReceiver> {
        self.retirement
            .receiver()
            .filter(|receiver| receiver.names(self))
    }
    pub fn is_returned(&self) -> bool {
        self.is_settled() || self.receiver().is_some()
    }
    pub async fn wait_returned_original(&self) -> Result<(), ChannelError> {
        self.retirement.wait_returned_original().await
    }
    pub async fn wait_settled_original(&self) -> Result<(), ChannelError> {
        self.retirement.wait_settled_original().await
    }
}
impl std::fmt::Debug for ProtocolOriginal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolOriginal")
            .field("instance", &self.instance())
            .field("settled", &self.is_settled())
            .finish_non_exhaustive()
    }
}

mod issuer;
mod receiver;
pub use receiver::{ProtocolOriginalReceiver, ProtocolReceiverPrepared};
