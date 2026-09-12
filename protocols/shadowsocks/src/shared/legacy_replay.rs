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

    pub(crate) fn with_tcp_capacity(mut self, capacity: Option<usize>) -> Self {
        if self.modern_capacity != capacity {
            self.modern_capacity = capacity;
            #[cfg(feature = "blake3")]
            {
                self.modern = Arc::new(crate::shared::ReplaySaltPool::configured(
                    crate::shared::ReplaySaltPool::DEFAULT_TTL,
                    capacity,
                ));
            }
        }
        self
    }
    #[cfg(feature = "blake3")]
    pub(crate) fn check_2022(&self, salt: &[u8]) -> Result<(), Error> {
        self.modern.check_and_insert(salt)
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
