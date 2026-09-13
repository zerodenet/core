//! A callback can dispatch while the runtime lives without owning its pools.
use super::ProtocolInventory;
use crate::protocol_registry::ProtocolRegistry;
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub(crate) struct WeakProtocolInventory(Weak<ProtocolRegistry>);

impl ProtocolInventory {
    pub(crate) fn downgrade(&self) -> WeakProtocolInventory {
        WeakProtocolInventory(Arc::downgrade(&self.registry))
    }
}

impl WeakProtocolInventory {
    pub(crate) fn upgrade(&self) -> Option<ProtocolInventory> {
        self.0
            .upgrade()
            .map(|registry| ProtocolInventory { registry })
    }
}
