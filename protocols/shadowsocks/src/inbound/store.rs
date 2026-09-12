use super::*;
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, Default)]
pub struct ShadowsocksInboundProfileStore {
    profiles: Arc<Mutex<HashMap<String, ShadowsocksInboundProfile>>>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundProfileStore {
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
