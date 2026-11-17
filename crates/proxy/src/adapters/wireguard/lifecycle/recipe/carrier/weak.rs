//! Same weak allocation, without recursively expanding every role's auto traits
//! through Carrier -> Options -> Carrier. No additional owner is installed.
use super::Carrier;
use std::sync::{Arc, Weak};
trait ActualCarrier: Send + Sync {
    fn actual(self: Arc<Self>) -> Arc<Carrier>;
}
impl ActualCarrier for Carrier {
    fn actual(self: Arc<Self>) -> Arc<Carrier> {
        self
    }
}
pub(in crate::adapters::wireguard::lifecycle) struct WeakCarrier(Weak<dyn ActualCarrier>);
impl WeakCarrier {
    pub(in crate::adapters::wireguard::lifecycle) fn new(carrier: &Arc<Carrier>) -> Self {
        let weak = Arc::downgrade(carrier);
        Self(weak)
    }
    pub(in crate::adapters::wireguard::lifecycle) fn upgrade(&self) -> Option<Arc<Carrier>> {
        self.0.upgrade().map(ActualCarrier::actual)
    }
}
