//! A callback can dispatch while the runtime lives without owning its pools.
use super::ProtocolInventory;
use crate::protocol_registry::ProtocolRegistry;
use std::sync::{Arc, Weak};

// Keep the exact weak allocation, but stop auto-trait expansion from retained
// service callbacks back through the entire device/Source graph. Upgrading
// returns that same allocation; neither view stores an additional owner.
trait ActualDevices: Send + Sync {
    fn actual(self: Arc<Self>) -> Arc<crate::adapters::devices::Devices>;
}
impl ActualDevices for crate::adapters::devices::Devices {
    fn actual(self: Arc<Self>) -> Arc<Self> {
        self
    }
}
#[cfg(feature = "raw-ip-runtime")]
type Publication = std::sync::Mutex<Option<Arc<super::device::publication::Publication>>>;
#[cfg(feature = "raw-ip-runtime")]
trait ActualPublication: Send + Sync {
    fn actual(self: Arc<Self>) -> Arc<Publication>;
}
#[cfg(feature = "raw-ip-runtime")]
impl ActualPublication for Publication {
    fn actual(self: Arc<Self>) -> Arc<Self> {
        self
    }
}

#[derive(Clone)]
pub(crate) struct WeakProtocolInventory {
    registry: Weak<ProtocolRegistry>,
    host: Weak<crate::runtime::RuntimeHost>,
    devices: Weak<dyn ActualDevices>,
    #[cfg(feature = "raw-ip-runtime")]
    device_publication: Weak<dyn ActualPublication>,
}

impl ProtocolInventory {
    pub(crate) fn downgrade(&self) -> WeakProtocolInventory {
        let devices = Arc::downgrade(&self.devices);
        #[cfg(feature = "raw-ip-runtime")]
        let device_publication = Arc::downgrade(&self.device_publication);
        WeakProtocolInventory {
            registry: Arc::downgrade(&self.registry),
            devices,
            #[cfg(feature = "raw-ip-runtime")]
            device_publication,
            host: Arc::downgrade(&self.host),
        }
    }
}

impl WeakProtocolInventory {
    pub(crate) fn upgrade(&self) -> Option<ProtocolInventory> {
        Some(ProtocolInventory {
            registry: self.registry.upgrade()?,
            devices: self.devices.upgrade()?.actual(),
            #[cfg(feature = "raw-ip-runtime")]
            device_publication: self.device_publication.upgrade()?.actual(),
            host: self.host.upgrade()?,
        })
    }
}
