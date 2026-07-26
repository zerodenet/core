//! Principal-scoped concurrent device accounting.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use zero_core::Address;

type DeviceCounts = HashMap<String, HashMap<Address, usize>>;

#[derive(Debug, Default)]
pub(crate) struct PrincipalDeviceRegistry {
    inner: Arc<Mutex<DeviceCounts>>,
}

impl PrincipalDeviceRegistry {
    pub(crate) fn acquire(
        &self,
        principal_key: &str,
        source_ip: Address,
        limit: u32,
    ) -> Option<PrincipalDeviceRegistration> {
        debug_assert!(limit > 0);

        let mut principals = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let devices = principals.entry(principal_key.to_owned()).or_default();
        if !devices.contains_key(&source_ip) && devices.len() >= limit as usize {
            return None;
        }
        *devices.entry(source_ip.clone()).or_default() += 1;

        Some(PrincipalDeviceRegistration {
            registry: Arc::downgrade(&self.inner),
            principal_key: principal_key.to_owned(),
            source_ip,
        })
    }

    #[cfg(test)]
    pub(crate) fn active_device_count(&self, principal_key: &str) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(principal_key)
            .map_or(0, HashMap::len)
    }
}

#[derive(Debug)]
pub struct PrincipalDeviceRegistration {
    registry: Weak<Mutex<DeviceCounts>>,
    principal_key: String,
    source_ip: Address,
}

impl Drop for PrincipalDeviceRegistration {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut principals = registry.lock().unwrap_or_else(|error| error.into_inner());
        let Some(devices) = principals.get_mut(&self.principal_key) else {
            return;
        };
        let Some(references) = devices.get_mut(&self.source_ip) else {
            return;
        };
        *references -= 1;
        if *references == 0 {
            devices.remove(&self.source_ip);
        }
        if devices.is_empty() {
            principals.remove(&self.principal_key);
        }
    }
}

#[cfg(test)]
mod tests;
