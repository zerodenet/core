//! Inert complete pool recipes, cold CA snapshots, and role binding helpers.

use super::*;
use std::num::NonZeroUsize;

impl<P: QuicPoolProtocol> QuicPool<P> {
    pub fn options(
        self: &Arc<Self>,
        profiles: Vec<P::Profile>,
        identity: Arc<[u8]>,
    ) -> QuicPoolOptions<P> {
        QuicPoolOptions {
            pool: Arc::downgrade(self),
            profiles: profiles.into(),
            identity,
            prepared_clients: Arc::new(OnceLock::new()),
            prepared_ca: None,
            limits: QuicPoolLimits {
                entry_limit: n(256),
                udp_sessions: n(4096),
                udp_pending_bytes: n(4 * 1024 * 1024),
                metadata_limits: default_metadata_limits(),
                endpoint_users: n(65536),
            },
        }
    }

    /// Pure role binding to the assembly's already prepared shared resource.
    /// Actual dependency generation and profile identity are checked on use.
    pub fn bind_prepared_profile(
        self: &Arc<Self>,
        profile: usize,
        owner: ComponentId,
        identity: Arc<[u8]>,
    ) -> QuicPoolRoleOptions<P> {
        QuicPoolRoleOptions {
            pool: Arc::downgrade(self),
            profile,
            owner,
            identity,
        }
    }
}

impl<P: QuicPoolProtocol> QuicPoolOptions<P> {
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
        self.limits.entry_limit = limit;
        Ok(self)
    }

    /// Shared UDP session registration and pending payload-byte admission.
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
        self.limits.udp_sessions = sessions;
        self.limits.udp_pending_bytes = bytes;
        Ok(self)
    }

    /// Cold budgets for this same authenticated pool; no new endpoint or original.
    pub fn with_metadata_limits(
        mut self,
        probes: NonZeroUsize,
        attempts: NonZeroUsize,
        sends: NonZeroUsize,
        users: NonZeroUsize,
    ) -> Result<Self, ChannelError> {
        if probes.get() > 256 || attempts.get() > 4096 || sends.get() > 4096 || users.get() > 65536
        {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-metadata-and-endpoint-users-v1");
        for value in [probes, attempts, sends, users] {
            identity.extend_from_slice(&value.get().to_le_bytes());
        }
        self.identity = identity.into();
        self.limits.metadata_limits.probe_sockets = probes;
        self.limits
            .metadata_limits
            .probe_attempts_per_initialization = attempts;
        self.limits.metadata_limits.concurrent_send_calls = sends;
        self.limits.endpoint_users = users;
        Ok(self)
    }

    /// Assembly supplies bytes only after confirming its original File closes.
    /// Without a snapshot the factory uses declared `ResourceDeclaration::File` access.
    pub fn with_prepared_ca(
        mut self,
        materials: Vec<Option<Arc<[u8]>>>,
    ) -> Result<Self, ChannelError> {
        if materials.len() != self.profiles.len()
            || self
                .profiles
                .iter()
                .zip(&materials)
                .any(|(profile, material)| {
                    P::profile_ca_cert_path(profile).is_some() != material.is_some()
                })
        {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        self.prepared_ca = Some(Arc::new(Mutex::new(Some(materials.into()))));
        Ok(self)
    }

    pub fn profiles(&self) -> &[P::Profile] {
        &self.profiles
    }

    pub fn limits(&self) -> &QuicPoolLimits {
        &self.limits
    }

    pub fn bind(&self, profile: usize, owner: ComponentId) -> QuicPoolRoleOptions<P> {
        QuicPoolRoleOptions {
            pool: self.pool.clone(),
            profile,
            owner,
            identity: P::profile_identity(&self.profiles[profile]).clone(),
        }
    }
}
