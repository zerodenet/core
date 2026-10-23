//! Shared profile publication: preparation owns values but cannot alter live users.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::{
    IntoTrojanInboundUserConfig, TrojanInboundProfile, TrojanInboundUserConfigParts,
    TrojanInboundUserRef,
};

#[derive(Debug, Clone, Default)]
pub struct TrojanInboundProfileStore {
    profiles: Arc<Mutex<HashMap<String, TrojanInboundProfile>>>,
}

/// Owned user replacement, inert until the host installs the prepared listener.
/// Dropping a candidate leaves the existing profile and its user snapshot intact.
pub struct TrojanInboundProfileUpdate {
    store: TrojanInboundProfileStore,
    tag: String,
    users: Vec<TrojanInboundUserConfigParts>,
}

impl TrojanInboundProfileStore {
    pub fn prepare(
        &self,
        tag: &str,
        users: &[TrojanInboundUserRef<'_>],
    ) -> TrojanInboundProfileUpdate {
        TrojanInboundProfileUpdate {
            store: self.clone(),
            tag: tag.to_owned(),
            users: users
                .iter()
                .copied()
                .map(IntoTrojanInboundUserConfig::into_trojan_inbound_user_config)
                .collect(),
        }
    }

    /// Publish the existing hot-reload update immediately at its caller's commit point.
    pub fn replace(&self, tag: &str, users: &[TrojanInboundUserRef<'_>]) -> TrojanInboundProfile {
        self.prepare(tag, users).commit()
    }
}

impl TrojanInboundProfileUpdate {
    /// Publish to the same actual profile, preserving all previously issued clones.
    /// Listener preparation must defer this until resource installation after promotion.
    pub fn commit(self) -> TrojanInboundProfile {
        let mut profiles = self
            .store
            .profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(profile) = profiles.get(&self.tag) {
            profile.replace_config_users(self.users);
            return profile.clone();
        }
        let profile = TrojanInboundProfile::from_config_users(self.users);
        profiles.insert(self.tag, profile.clone());
        profile
    }
}
