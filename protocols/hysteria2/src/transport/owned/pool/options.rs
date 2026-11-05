//! Inert complete pool recipes and actual owner budget identity.
use super::*;

impl Pool {
    pub fn options(
        self: &Arc<Self>,
        profiles: Vec<PoolProfile>,
        identity: Arc<[u8]>,
    ) -> PoolOptions {
        PoolOptions {
            pool: Arc::downgrade(self),
            profiles: profiles.into(),
            identity,
            prepared_clients: Arc::new(OnceLock::new()),
            prepared_ca: None,
            entry_limit: n(256),
            udp_sessions: n(4096),
            udp_pending_bytes: n(4 * 1024 * 1024),
        }
    }
}
impl PoolOptions {
    /// Bounds the same actual pool and its retained failure evidence. No
    /// endpoint, original, worker or logical user is allocated by this recipe.
    pub fn with_entry_limit(mut self, limit: NonZeroUsize) -> Result<Self, ChannelError> {
        if limit.get() > 256 {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-entry-and-failure-budget-v1");
        identity.extend_from_slice(&limit.get().to_le_bytes());
        self.identity = identity.into();
        self.entry_limit = limit;
        Ok(self)
    }
    /// A single dispatcher shares its actual payload-byte admission across
    /// every logical registration and incomplete fragment assembly.
    pub fn with_udp_limits(
        mut self,
        sessions: NonZeroUsize,
        bytes: NonZeroUsize,
    ) -> Result<Self, ChannelError> {
        if sessions.get() > 4096 || bytes.get() > 64 * 1024 * 1024 {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-udp-registration-and-payload-v1");
        identity.extend_from_slice(&sessions.get().to_le_bytes());
        identity.extend_from_slice(&bytes.get().to_le_bytes());
        self.identity = identity.into();
        self.udp_sessions = sessions;
        self.udp_pending_bytes = bytes;
        Ok(self)
    }
    pub fn profiles(&self) -> &[PoolProfile] {
        &self.profiles
    }
    pub fn bind(&self, profile: usize, owner: ComponentId) -> RoleOptions {
        RoleOptions {
            pool: self.pool.clone(),
            profile,
            owner,
            identity: self.profiles[profile].identity.clone(),
        }
    }
}
