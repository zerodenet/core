//! Cold options keep only the same cache root and budget proposals.
use super::*;
pub struct UdpPoolOptions {
    pub(in crate::component::udp) root: Weak<PoolRoot>,
    pub(in crate::component::udp) resume: ShadowsocksManagedDatagramFlowResume,
    pub(in crate::component::udp) endpoint: Destination,
    pub(in crate::component::udp) binding: UpstreamBinding,
    pub(super) identity: Arc<[u8]>,
    base_identity: Arc<[u8]>,
    pub(super) legacy_budget: Option<crate::shared::LegacyReplayBudgetProposal>,
    #[cfg(feature = "blake3")]
    pub(super) udp_budget: Option<crate::udp::UdpBudgetProposal>,
    pub(super) pending: crate::transport::udp_socket::responses::budget::PendingProposal,
    pub(in crate::component::udp) process: Option<(ComponentId, std::time::Duration)>,
}
impl UdpPoolOptions {
    pub(crate) fn new(
        mut resume: ShadowsocksManagedDatagramFlowResume,
        endpoint: Destination,
        binding: UpstreamBinding,
    ) -> Result<Self, zero_core::Error> {
        let selected = resume.root.take().ok_or(zero_core::Error::Config(
            "UDP pool options require the adapter's original cache root",
        ))?;
        let mut identity = resume.protocol.flow_cache_key().into_bytes();
        identity.extend_from_slice(format!("{endpoint:?}{binding:?}").as_bytes());
        let pending = crate::transport::udp_socket::responses::budget::PendingProposal::new(
            &selected.root.budget,
            Default::default(),
        )
        .map_err(|_| zero_core::Error::Config("invalid actual SS UDP pending budget"))?;
        let base_identity: Arc<[u8]> = identity.into();
        let mut options = Self {
            root: Arc::downgrade(&selected.root),
            resume,
            endpoint,
            binding,
            identity: base_identity.clone(),
            base_identity,
            pending,
            legacy_budget: None,
            #[cfg(feature = "blake3")]
            udp_budget: None,
            process: None,
        };
        options.rebuild_identity();
        Ok(options)
    }
    #[cfg(feature = "blake3")]
    pub fn with_security_budget_proposals(
        mut self,
        udp: crate::udp::UdpBudgetProposal,
        legacy: crate::shared::LegacyReplayBudgetProposal,
    ) -> Self {
        self.udp_budget = Some(udp);
        self.legacy_budget = Some(legacy);
        self.rebuild_identity();
        self
    }
    pub fn with_process_dependency(
        mut self,
        id: ComponentId,
        startup: std::time::Duration,
    ) -> Self {
        self.process = Some((id, startup));
        self.rebuild_identity();
        self
    }
    pub fn with_pending_limits(
        mut self,
        limits: crate::transport::ShadowsocksUdpPendingLimits,
    ) -> Result<Self, zero_core::Error> {
        let root = self.root.upgrade().ok_or(zero_core::Error::Protocol(
            "the original SS UDP cache root was released",
        ))?;
        self.pending = crate::transport::udp_socket::responses::budget::PendingProposal::new(
            &root.budget,
            limits,
        )
        .map_err(|_| zero_core::Error::Config("invalid SS UDP pending response limits"))?;
        self.rebuild_identity();
        Ok(self)
    }
    fn rebuild_identity(&mut self) {
        let mut identity = self.base_identity.to_vec();
        if let Some((id, startup)) = self.process {
            identity.extend_from_slice(b"same-process-udp");
            identity.extend_from_slice(&id.0.to_le_bytes());
            identity.extend_from_slice(&startup.as_nanos().to_le_bytes());
        }
        identity.extend_from_slice(&self.pending.limits.per_cache.to_le_bytes());
        identity.extend_from_slice(&self.pending.limits.total.to_le_bytes());
        use std::hash::{BuildHasher, Hash, Hasher};
        static KEYS: std::sync::OnceLock<[std::collections::hash_map::RandomState; 2]> =
            std::sync::OnceLock::new();
        for key in KEYS.get_or_init(|| [Default::default(), Default::default()]) {
            let mut hash = key.build_hasher();
            self.legacy_budget
                .as_ref()
                .map(|view| view.identity())
                .hash(&mut hash);
            #[cfg(feature = "blake3")]
            self.udp_budget
                .as_ref()
                .map(|view| view.identity())
                .hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        self.identity = identity.into();
    }
    pub fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.binding
    }
}
