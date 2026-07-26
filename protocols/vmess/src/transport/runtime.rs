use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use zero_core::Error;

use super::VmessInboundUserRef;

#[derive(Debug, Clone, Default)]
pub struct VmessTransportRuntime {
    mux_pool: crate::mux::VmessMuxConnectionPool,
    inbound_profiles: Arc<Mutex<HashMap<String, crate::inbound::VmessInboundProfile>>>,
}

impl VmessTransportRuntime {
    pub fn on_config_reloaded(&self) {
        self.mux_pool.evict_all();
    }

    pub(super) fn mux_pool(&self) -> crate::mux::VmessMuxConnectionPool {
        self.mux_pool.clone()
    }

    pub fn replace_inbound_profile(
        &self,
        tag: &str,
        users: &[VmessInboundUserRef<'_>],
    ) -> Result<crate::inbound::VmessInboundProfile, Error> {
        let mut profiles = self
            .inbound_profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(profile) = profiles.get(tag) {
            profile.replace_config_users(users.iter().copied())?;
            return Ok(profile.clone());
        }

        let profile =
            crate::inbound::VmessInboundProfile::from_config_users(users.iter().copied())?;
        profiles.insert(tag.to_owned(), profile.clone());
        Ok(profile)
    }
}
