use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use zero_core::Error;

use super::VmessInboundUserRef;

#[derive(Debug, Clone, Default)]
pub struct VmessTransportRuntime {
    grpc_carriers: Arc<crate::component::grpc::registry::State>,
    mux_pool: crate::mux::VmessMuxConnectionPool,
    inbound_profiles: Arc<Mutex<HashMap<String, crate::inbound::VmessInboundProfile>>>,
}

impl VmessTransportRuntime {
    pub(crate) fn grpc_carrier_state(&self) -> Arc<crate::component::grpc::registry::State> {
        self.grpc_carriers.clone()
    }
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
        let users =
            crate::inbound::PreparedVmessInboundUsers::from_config_users(users.iter().copied())?;
        self.stage_inbound_profile(tag, users).activate()
    }

    /// Cold typed preparation: no actual profile is created or modified here.
    pub fn stage_inbound_profile(
        &self,
        tag: &str,
        users: crate::inbound::PreparedVmessInboundUsers,
    ) -> PreparedVmessInboundProfile {
        PreparedVmessInboundProfile {
            registry: Arc::downgrade(&self.inbound_profiles),
            tag: tag.into(),
            users,
        }
    }
}

/// Publish only at the host's accepted installation point, or through the
/// existing explicitly hot replace_inbound_profile API after reconciliation.
/// The weak source cannot recreate a released adapter/runtime owner.
pub struct PreparedVmessInboundProfile {
    registry: std::sync::Weak<Mutex<HashMap<String, crate::inbound::VmessInboundProfile>>>,
    tag: String,
    users: crate::inbound::PreparedVmessInboundUsers,
}
impl PreparedVmessInboundProfile {
    pub fn activate(self) -> Result<crate::inbound::VmessInboundProfile, Error> {
        let registry = self
            .registry
            .upgrade()
            .ok_or(Error::Config("VMess inbound profile runtime was released"))?;
        let mut profiles = registry.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(profile) = profiles.get(&self.tag) {
            self.users.publish(profile);
            return Ok(profile.clone());
        }
        let profile = self.users.into_profile();
        profiles.insert(self.tag, profile.clone());
        Ok(profile)
    }
}
