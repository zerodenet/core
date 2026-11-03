//! Actual original socket local address and selected peer. TCP records its
//! connected peer; UDP records the selected send peer, without remote confirmation.
use super::{slot::Slot, CarrierAdoption};
use std::{
    net::SocketAddr,
    sync::{Arc, Weak},
};
#[derive(Clone, Debug)]
pub struct CarrierEndpoints {
    pub(super) origin: EndpointOriginal,
    pub(super) local: Option<SocketAddr>,
    pub(super) peer: SocketAddr,
}
#[derive(Clone)]
pub(super) enum EndpointOriginal {
    Carrier(Weak<Slot>),
    Metadata(crate::controlled_datagram::metadata_pool::MetadataPoolOrigin),
}
impl std::fmt::Debug for EndpointOriginal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Carrier(_) => "Carrier",
            Self::Metadata(_) => "Metadata",
        })
    }
}
impl CarrierEndpoints {
    pub(super) fn metadata(
        original: crate::controlled_datagram::metadata_pool::MetadataPoolOrigin,
        local: SocketAddr,
        peer: SocketAddr,
    ) -> Self {
        Self {
            origin: EndpointOriginal::Metadata(original),
            local: Some(local),
            peer,
        }
    }
    pub fn matches_metadata(
        &self,
        original: &crate::controlled_datagram::metadata_pool::MetadataPoolOrigin,
    ) -> bool {
        matches!(&self.origin, EndpointOriginal::Metadata(actual) if actual.same_original(original))
    }

    pub fn local(&self) -> Option<SocketAddr> {
        self.local
    }
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
    pub fn matches(&self, adoption: &CarrierAdoption) -> bool {
        matches!(&self.origin, EndpointOriginal::Carrier(origin) if Weak::ptr_eq(origin, &Arc::downgrade(&adoption.slot)))
    }
}
/// Called outside original native/carrier locks after ready metadata is durable.
pub trait CarrierEndpointObserver: Send + Sync {
    fn connected(&self, endpoints: &CarrierEndpoints);
}
