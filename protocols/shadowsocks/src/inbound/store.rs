use super::*;
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, Default)]
pub struct ShadowsocksInboundProfileStore {
    profiles: Arc<Mutex<HashMap<String, ShadowsocksInboundProfile>>>,
}

#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct PreparedInboundProfile {
    store: ShadowsocksInboundProfileStore,
    tag: String,
    profile: ShadowsocksInboundProfile,
}
impl PreparedInboundProfile {
    pub(crate) fn needs_replay_task(&self) -> bool {
        #[cfg(feature = "blake3")]
        {
            self.profile.cipher.is_blake3()
        }
        #[cfg(not(feature = "blake3"))]
        {
            false
        }
    }

    /// Called by the owning component only after confirmed StopOld.
    pub fn activate(self) -> ShadowsocksInboundProfile {
        let mut profiles = self
            .store
            .profiles
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut profile = self.profile;
        // A cold recipe must not share a mutable credential authority with a live source.
        profile.users = Arc::new(std::sync::RwLock::new(profile.users_snapshot()));
        if let Some(previous) = profiles.get(&self.tag) {
            if previous.cipher_name == profile.cipher_name
                && previous.identity_password == profile.identity_password
            {
                *previous.users.write().unwrap_or_else(|e| e.into_inner()) =
                    profile.users_snapshot();
                profile.users = previous.users.clone();
            }
        }
        profiles.insert(self.tag, profile.clone());
        profile
    }
}
#[cfg(feature = "crypto")]
impl ShadowsocksInboundProfileStore {
    pub fn prepare_with_identity(
        &self,
        tag: &str,
        cipher: &str,
        identity_password: Option<&str>,
        users: &[ShadowsocksInboundUserRef<'_>],
        replay: crate::validation::ReplayPolicy,
        limits: crate::validation::StateLimits,
    ) -> Result<PreparedInboundProfile, Error> {
        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            cipher,
            identity_password,
            users.iter().copied(),
        )?
        .with_replay_policy(replay)
        .with_state_limits(limits);
        Ok(PreparedInboundProfile {
            store: self.clone(),
            tag: tag.into(),
            profile,
        })
    }

    pub fn replace(
        &self,
        tag: &str,
        cipher: &str,
        users: &[ShadowsocksInboundUserRef<'_>],
    ) -> Result<ShadowsocksInboundProfile, Error> {
        self.replace_with_identity(tag, cipher, None, users)
    }

    pub fn replace_with_identity(
        &self,
        tag: &str,
        cipher: &str,
        identity_password: Option<&str>,
        users: &[ShadowsocksInboundUserRef<'_>],
    ) -> Result<ShadowsocksInboundProfile, Error> {
        let mut profiles = self
            .profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(profile) = profiles.get(tag) {
            if profile.cipher_name() == cipher
                && profile.identity_password() == identity_password.map(str::as_bytes)
            {
                profile.replace_config_users(users.iter().copied())?;
                return Ok(profile.clone());
            }
        }

        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            cipher,
            identity_password,
            users.iter().copied(),
        )?;
        profiles.insert(tag.to_owned(), profile.clone());
        Ok(profile)
    }
}

#[cfg(feature = "crypto")]
pub fn inbound_profile_from_config_cipher_password(
    cipher_name: &str,
    password: &str,
) -> Result<ShadowsocksInboundProfile, Error> {
    ShadowsocksInboundProfile::from_config_cipher_password(cipher_name, password)
}
