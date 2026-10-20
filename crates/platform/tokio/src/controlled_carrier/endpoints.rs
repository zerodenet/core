//! Ready-time facts from the same original connected socket, never configured guesses.
use super::{slot::Slot, CarrierAdoption};
use std::{
    net::SocketAddr,
    sync::{Arc, Weak},
};
#[derive(Clone, Debug)]
pub struct CarrierEndpoints {
    pub(super) origin: Weak<Slot>,
    pub(super) local: Option<SocketAddr>,
    pub(super) peer: SocketAddr,
}
impl CarrierEndpoints {
    pub fn local(&self) -> Option<SocketAddr> {
        self.local
    }
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
    pub fn matches(&self, adoption: &CarrierAdoption) -> bool {
        Weak::ptr_eq(&self.origin, &Arc::downgrade(&adoption.slot))
    }
}
/// Called outside original native/carrier locks after ready metadata is durable.
pub trait CarrierEndpointObserver: Send + Sync {
    fn connected(&self, endpoints: &CarrierEndpoints);
}
