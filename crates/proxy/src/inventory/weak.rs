//! A callback can dispatch while the runtime lives without owning its pools.
use super::ProtocolInventory;
use crate::protocol_registry::ProtocolRegistry;
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub(crate) struct WeakProtocolInventory {
    registry: Weak<ProtocolRegistry>,
    host: Weak<crate::runtime::RuntimeHost>,
    devices: Weak<crate::adapters::devices::Devices>,
    #[cfg(feature = "raw-ip-runtime")]
    device_publication:
        Weak<std::sync::Mutex<Option<Arc<super::device::publication::Publication>>>>,
}

impl ProtocolInventory {
    pub(crate) fn downgrade(&self) -> WeakProtocolInventory {
        WeakProtocolInventory {
            registry: Arc::downgrade(&self.registry),
            devices: Arc::downgrade(&self.devices),
            #[cfg(feature = "raw-ip-runtime")]
            device_publication: Arc::downgrade(&self.device_publication),
            host: Arc::downgrade(&self.host),
        }
    }
}

impl WeakProtocolInventory {
    pub(crate) fn upgrade(&self) -> Option<ProtocolInventory> {
        Some(ProtocolInventory {
            registry: self.registry.upgrade()?,
            devices: self.devices.upgrade()?,
            #[cfg(feature = "raw-ip-runtime")]
            device_publication: self.device_publication.upgrade()?,
            host: self.host.upgrade()?,
        })
    }
}
