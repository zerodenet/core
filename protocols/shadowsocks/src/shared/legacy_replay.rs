//! Legacy replay policy matching the fixed reference's two rotating Bloom filters.
use crate::validation::ReplayPolicy;
use bloomfilter::Bloom;
use std::sync::{Arc, Mutex};
use zero_core::Error;

struct Window {
    filters: [Bloom<[u8]>; 2],
    counts: [usize; 2],
    capacity: usize,
    current: usize,
}
impl Window {
    fn new(server: bool) -> Self {
        let (capacity, probability) = if server {
            (500_000, 1e-6)
        } else {
            (5_000, 1e-15)
        };
        Self {
            filters: [
                Bloom::new_for_fp_rate(capacity, probability),
                Bloom::new_for_fp_rate(capacity, probability),
            ],
            counts: [0, 0],
            capacity,
            current: 0,
        }
    }
    fn duplicate(&mut self, salt: &[u8]) -> bool {
        if self.filters.iter().any(|filter| filter.check(salt)) {
            return true;
        }
        if self.counts[self.current] >= self.capacity {
            self.current ^= 1;
            self.filters[self.current].clear();
            self.counts[self.current] = 0;
        }
        self.filters[self.current].set(salt);
        self.counts[self.current] += 1;
        false
    }
}
#[derive(Clone)]
pub(crate) struct LegacyReplay {
    policy: ReplayPolicy,
    server: bool,
    modern_capacity: Option<usize>,
    #[cfg(feature = "blake3")]
    modern: Arc<crate::shared::ReplaySaltPool>,
    #[cfg(feature = "blake3")]
    udp_budget: Arc<crate::udp::UdpReplayBudget>,
    #[cfg(feature = "blake3")]
    udp_limits: Option<crate::udp::UdpBudgetLimits>,
    #[cfg(feature = "blake3")]
    udp_received:
        Arc<Mutex<std::collections::HashMap<String, crate::udp::session::ReceiveWindows>>>,
    #[cfg(feature = "blake3")]
    udp_responses:
        Arc<Mutex<std::collections::HashMap<String, crate::udp::session::ResponseSessions>>>,
    // Allocate only when an enabled policy sees its first authenticated salt.
    state: Arc<Mutex<Option<Window>>>,
}
impl LegacyReplay {
    pub(crate) fn generate(&self, length: usize) -> Result<Vec<u8>, Error> {
        let mut nonce = vec![0; length];
        if length == 0 {
            return Ok(nonce);
        }
        loop {
            crate::shared::fill_random(&mut nonce)?;
            if nonce.iter().all(|byte| *byte == 0) {
                continue;
            }
            if self.policy == ReplayPolicy::Ignore {
                return Ok(nonce);
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Protocol("ss: legacy replay state poisoned"))?;
            if !state
                .get_or_insert_with(|| Window::new(self.server))
                .duplicate(&nonce)
            {
                return Ok(nonce);
            }
        }
    }

    pub(crate) fn with_policy(mut self, policy: ReplayPolicy) -> Self {
        self.policy = policy;
        self
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn with_modern_history(mut self, pool: &crate::shared::ReplaySaltPool) -> Self {
        self.modern = Arc::new(pool.alias(self.modern_capacity));
        self
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn with_udp_replay_budget(
        mut self,
        budget: Arc<crate::udp::UdpReplayBudget>,
    ) -> Result<Self, Error> {
        if !Arc::ptr_eq(&self.udp_budget, &budget)
            && (!self
                .udp_received
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
                || !self
                    .udp_responses
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_empty())
        {
            return Err(Error::Config("cannot fork retained SS UDP security books"));
        }
        self.udp_budget = budget;
        Ok(self)
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn with_udp_budget_limits(
        mut self,
        limits: crate::udp::UdpBudgetLimits,
    ) -> Result<Self, Error> {
        self.udp_budget.validate(limits)?;
        self.udp_limits = Some(limits);
        Ok(self)
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn udp_budget_identity(&self) -> (usize, Option<crate::udp::UdpBudgetLimits>) {
        (Arc::as_ptr(&self.udp_budget) as usize, self.udp_limits)
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn activate_udp_budget(&self) -> Result<(), Error> {
        if let Some(limits) = self.udp_limits {
            self.udp_budget.install(limits)?;
        }
        Ok(())
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn udp_replay_budget(&self) -> Arc<crate::udp::UdpReplayBudget> {
        self.udp_budget.clone()
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn received_for_user(
        &self,
        key: String,
        limits: crate::validation::StateLimits,
    ) -> crate::udp::session::ReceiveWindows {
        let mut books = self
            .udp_received
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        books.retain(|_, original| !original.unused());
        books
            .entry(key)
            .or_insert_with(|| {
                crate::udp::session::ReceiveWindows::with_budget(limits, self.udp_budget.clone())
            })
            .clone()
            .with_limits_view(limits)
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn responses_for_user(
        &self,
        key: String,
        limits: crate::validation::StateLimits,
    ) -> crate::udp::session::ResponseSessions {
        let mut books = self
            .udp_responses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        books.retain(|_, original| !original.unused());
        let mut original = books
            .entry(key)
            .or_insert_with(|| {
                crate::udp::session::ResponseSessions::with_budget(limits, self.udp_budget.clone())
            })
            .clone();
        original.set_limits(limits);
        original
    }
    pub(crate) fn with_tcp_capacity(mut self, capacity: Option<usize>) -> Self {
        if self.modern_capacity != capacity {
            self.modern_capacity = capacity;
            #[cfg(feature = "blake3")]
            {
                self.modern = Arc::new(self.modern.alias(capacity));
            }
        }
        self
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn check_2022(&self, salt: &[u8]) -> Result<(), Error> {
        self.modern.check_and_insert(salt)
    }

    #[cfg(feature = "blake3")]
    pub(crate) fn modern_pool(&self) -> Arc<crate::shared::ReplaySaltPool> {
        self.modern.clone()
    }

    pub(crate) fn policy(&self) -> ReplayPolicy {
        self.policy
    }
    pub(crate) fn new(policy: ReplayPolicy, server: bool) -> Self {
        Self {
            policy,
            server,
            modern_capacity: None,
            #[cfg(feature = "blake3")]
            modern: Arc::new(crate::shared::ReplaySaltPool::new()),
            #[cfg(feature = "blake3")]
            udp_budget: Default::default(),
            #[cfg(feature = "blake3")]
            udp_limits: None,
            #[cfg(feature = "blake3")]
            udp_received: Default::default(),
            #[cfg(feature = "blake3")]
            udp_responses: Default::default(),
            state: Default::default(),
        }
    }
    pub(crate) fn check(&self, salt: &[u8]) -> Result<(), Error> {
        if salt.is_empty() || matches!(self.policy, ReplayPolicy::Default | ReplayPolicy::Ignore) {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("ss: legacy replay state poisoned"))?;
        if state
            .get_or_insert_with(|| Window::new(self.server))
            .duplicate(salt)
        {
            if self.policy == ReplayPolicy::Reject {
                return Err(Error::Protocol("ss: legacy replay rejected"));
            }
            tracing::warn!("ss: detected repeated legacy nonce");
        }
        Ok(())
    }
}
impl std::fmt::Debug for LegacyReplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LegacyReplay")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}
impl PartialEq for LegacyReplay {
    fn eq(&self, other: &Self) -> bool {
        self.policy == other.policy
            && self.server == other.server
            && self.modern_capacity == other.modern_capacity
    }
}
impl Eq for LegacyReplay {}
