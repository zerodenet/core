use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use zero_core::Error;

use super::VlessInboundUserRef;

#[derive(Debug, Clone, Default)]
pub struct VlessTransportRuntime {
    mux_pool: crate::mux_pool::MuxConnectionPool,
    inbound_profiles: Arc<Mutex<HashMap<String, crate::inbound::VlessInboundProfile>>>,
}

impl VlessTransportRuntime {
    pub fn on_config_reloaded(&self) {
        self.mux_pool.evict_all();
    }

    pub(super) fn mux_pool(&self) -> crate::mux_pool::MuxConnectionPool {
        self.mux_pool.clone()
    }

    pub fn replace_inbound_profile(
        &self,
        tag: &str,
        users: &[VlessInboundUserRef<'_>],
    ) -> Result<crate::inbound::VlessInboundProfile, Error> {
        let mut profiles = self
            .inbound_profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(profile) = profiles.get(tag) {
            profile.replace_config_users(users.iter().copied())?;
            return Ok(profile.clone());
        }

        let profile =
            crate::inbound::VlessInboundProfile::from_config_users(users.iter().copied())?;
        profiles.insert(tag.to_owned(), profile.clone());
        Ok(profile)
    }
}
