//! The SAME immutable Service facts, including owned allocation custody.
//! Deref exposes facts only. A Paid raw Arc/Weak can never escape this handle.
use std::{ops::Deref, sync::Arc};
use zero_core::request::ServiceRequestFacts;
use zero_traits::packet_metadata::{
    PacketMemoryError, PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout,
};
#[derive(Clone)]
pub struct ServiceFactsOwner(Storage);
#[derive(Clone)]
enum Storage {
    Shared(Arc<ServiceRequestFacts>),
    Paid(PacketMetadataArc<ServiceRequestFacts>),
}
impl ServiceFactsOwner {
    pub fn paid_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataArc::<ServiceRequestFacts>::layout()
    }
    pub fn from_prepaid(
        facts: ServiceRequestFacts,
        credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, ServiceRequestFacts, PacketMetadataCredit)> {
        PacketMetadataArc::from_prepaid(facts, credit).map(|actual| Self(Storage::Paid(actual)))
    }
    /// Exact actual immutable owner, never equivalence of copied facts.
    pub fn same_owner(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Storage::Shared(a), Storage::Shared(b)) => Arc::ptr_eq(a, b),
            (Storage::Paid(a), Storage::Paid(b)) => PacketMetadataArc::ptr_eq(a, b),
            _ => false,
        }
    }
    /// An existing actual Probe/Service owner retains its original foreign Arc.
    /// This observes that owner without granting anything or charging it twice.
    pub fn matches_shared(&self, actual: &Arc<ServiceRequestFacts>) -> bool {
        matches!(&self.0,Storage::Shared(same) if Arc::ptr_eq(same,actual))
    }
}
impl From<Arc<ServiceRequestFacts>> for ServiceFactsOwner {
    fn from(actual: Arc<ServiceRequestFacts>) -> Self {
        Self(Storage::Shared(actual))
    }
}
impl Deref for ServiceFactsOwner {
    type Target = ServiceRequestFacts;
    fn deref(&self) -> &Self::Target {
        match &self.0 {
            Storage::Shared(actual) => actual,
            Storage::Paid(actual) => actual,
        }
    }
}
impl std::fmt::Debug for ServiceFactsOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.deref(), f)
    }
}

impl AsRef<ServiceRequestFacts> for ServiceFactsOwner {
    fn as_ref(&self) -> &ServiceRequestFacts {
        self
    }
}
