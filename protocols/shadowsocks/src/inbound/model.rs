use super::*;
/// Protocol-owned validated inbound profile.
///
/// Proxy runtime code keeps this as an opaque profile and delegates TCP/UDP
/// Shadowsocks framing decisions back to the protocol crate.
#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct ShadowsocksInboundProfile {
    pub(super) cipher_name: String,
    pub(super) cipher: crate::shared::CipherKind,
    pub(super) identity_password: Option<Vec<u8>>,
    pub(super) users: Arc<RwLock<Arc<ShadowsocksAuthorizedUsers>>>,
    pub(super) replay: crate::shared::legacy_replay::LegacyReplay,
    pub(super) limits: crate::validation::StateLimits,
}

#[cfg(feature = "crypto")]
#[derive(Clone, PartialEq, Eq)]
pub struct ShadowsocksUser {
    pub(super) password: Vec<u8>,
    pub(super) principal_key: Option<String>,
    pub(super) up_bps: Option<u64>,
    pub(super) down_bps: Option<u64>,
    pub(super) device_limit: Option<u32>,
    pub(super) quota_remaining_bytes: Option<u64>,
    pub(super) policy_revision: Option<u64>,
}

#[cfg(feature = "crypto")]
#[derive(Debug)]
pub(crate) struct ShadowsocksAuthorizedUsers {
    pub(super) users: Arc<[ShadowsocksUser]>,
    #[cfg(feature = "blake3")]
    pub(super) identities: HashMap<[u8; 16], usize>,
}

#[cfg(feature = "crypto")]
impl core::ops::Deref for ShadowsocksAuthorizedUsers {
    type Target = [ShadowsocksUser];

    fn deref(&self) -> &Self::Target {
        &self.users
    }
}

#[cfg(feature = "crypto")]
#[derive(Clone, Copy)]
pub struct ShadowsocksInboundUserRef<'a> {
    pub password: &'a str,
    pub principal_key: Option<&'a str>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
}

#[cfg(feature = "crypto")]
impl core::fmt::Debug for ShadowsocksInboundProfile {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ShadowsocksInboundProfile")
            .field("cipher_name", &self.cipher_name)
            .field("user_count", &self.user_count())
            .finish()
    }
}

#[cfg(feature = "crypto")]
impl ShadowsocksUser {
    fn from_ref(user: ShadowsocksInboundUserRef<'_>) -> Self {
        Self {
            password: user.password.as_bytes().to_vec(),
            principal_key: user.principal_key.map(String::from),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
            device_limit: user.device_limit,
            quota_remaining_bytes: user.quota_remaining_bytes,
            policy_revision: user.policy_revision,
        }
    }

    pub(crate) fn password(&self) -> &[u8] {
        &self.password
    }

    pub(crate) fn cache_key(&self) -> String {
        crate::shared::cache_identity([self.password.as_slice()])
    }

    pub(crate) fn auth(&self) -> SessionAuth {
        let mut auth = SessionAuth::new("shadowsocks");
        auth.principal_key = self
            .principal_key
            .clone()
            .or_else(|| Some(self.cache_key()));
        auth.up_bps = self.up_bps;
        auth.down_bps = self.down_bps;
        auth.device_limit = self.device_limit;
        auth.quota_remaining_bytes = self.quota_remaining_bytes;
        auth.policy_revision = self.policy_revision;
        auth
    }

    #[cfg(feature = "blake3")]
    fn identity_hash(&self, cipher: crate::shared::CipherKind) -> Result<[u8; 16], Error> {
        crate::shared::identity_hash_2022(cipher, &self.password)
    }
}

#[cfg(feature = "crypto")]
impl ShadowsocksAuthorizedUsers {
    pub(super) fn from_refs<'a, I>(
        _cipher: crate::shared::CipherKind,
        users: I,
    ) -> Result<Self, Error>
    where
        I: IntoIterator<Item = ShadowsocksInboundUserRef<'a>>,
    {
        let users: Arc<[ShadowsocksUser]> = users
            .into_iter()
            .map(ShadowsocksUser::from_ref)
            .collect::<Vec<_>>()
            .into();
        #[cfg(feature = "blake3")]
        let identities = if _cipher.is_blake3() {
            let mut identities = HashMap::with_capacity(users.len());
            for (index, user) in users.iter().enumerate() {
                let identity = user.identity_hash(_cipher)?;
                if identities.insert(identity, index).is_some() {
                    return Err(Error::Protocol("ss: duplicate SIP023 user identity"));
                }
            }
            identities
        } else {
            HashMap::new()
        };
        Ok(Self {
            users,
            #[cfg(feature = "blake3")]
            identities,
        })
    }

    #[cfg(feature = "blake3")]
    pub(crate) fn find_identity(&self, identity: &[u8; 16]) -> Option<(usize, &ShadowsocksUser)> {
        let index = *self.identities.get(identity)?;
        self.users.get(index).map(|user| (index, user))
    }
}

impl core::fmt::Debug for ShadowsocksUser {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksUser")
            .field("principal_key", &self.principal_key)
            .finish_non_exhaustive()
    }
}

impl core::fmt::Debug for ShadowsocksInboundUserRef<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksInboundUserRef")
            .field("principal_key", &self.principal_key)
            .finish_non_exhaustive()
    }
}
