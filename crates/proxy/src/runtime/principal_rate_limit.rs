use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use zero_core::Session;

use crate::transport::SharedRateLimiter;

/// Proxy-lifetime registry for Zero's principal-wide traffic shaping policy.
///
/// Entries are weak so a principal policy disappears after its final TCP or
/// UDP flow releases the corresponding [`TrafficRateLimiters`].
#[derive(Debug, Clone, Default)]
pub(crate) struct PrincipalRateLimitRegistry {
    inner: Arc<PrincipalRateLimitRegistryInner>,
}

#[derive(Debug, Default)]
struct PrincipalRateLimitRegistryInner {
    entries: Mutex<HashMap<PrincipalRatePolicyKey, PrincipalRateLimitEntry>>,
    next_generation: AtomicU64,
}

#[derive(Debug)]
struct PrincipalRateLimitEntry {
    generation: u64,
    lease: Weak<TrafficRateLimitLease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PrincipalRatePolicyKey {
    principal_key: String,
    policy_revision: Option<u64>,
    upload_bps: Option<u64>,
    download_bps: Option<u64>,
}

#[derive(Debug, Default)]
struct TrafficRateLimitState {
    upload: Option<SharedRateLimiter>,
    download: Option<SharedRateLimiter>,
}

#[derive(Debug)]
struct TrafficRateLimitLease {
    state: TrafficRateLimitState,
    registration: Option<PrincipalRateLimitRegistration>,
}

#[derive(Debug)]
struct PrincipalRateLimitRegistration {
    registry: Weak<PrincipalRateLimitRegistryInner>,
    key: PrincipalRatePolicyKey,
    generation: u64,
}

/// Bidirectional shaping handles for one authenticated principal policy or
/// one anonymous session.
#[derive(Debug, Clone)]
pub(crate) struct TrafficRateLimiters {
    lease: Arc<TrafficRateLimitLease>,
}

impl PrincipalRateLimitRegistry {
    pub(crate) fn acquire(&self, session: &Session) -> TrafficRateLimiters {
        let upload_bps = normalize_rate(session.up_bps);
        let download_bps = normalize_rate(session.down_bps);
        if upload_bps.is_none() && download_bps.is_none() {
            return TrafficRateLimiters::default();
        }

        let Some(principal_key) = session
            .auth
            .as_ref()
            .and_then(|auth| auth.principal_key.as_ref())
        else {
            return TrafficRateLimiters::new(upload_bps, download_bps);
        };
        let key = PrincipalRatePolicyKey {
            principal_key: principal_key.clone(),
            policy_revision: session.auth.as_ref().and_then(|auth| auth.policy_revision),
            upload_bps,
            download_bps,
        };

        let mut entries = self
            .inner
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(lease) = entries.get(&key).and_then(|entry| entry.lease.upgrade()) {
            return TrafficRateLimiters { lease };
        }

        let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
        let limiters = TrafficRateLimiters::registered(
            upload_bps,
            download_bps,
            Arc::downgrade(&self.inner),
            key.clone(),
            generation,
        );
        entries.insert(
            key,
            PrincipalRateLimitEntry {
                generation,
                lease: Arc::downgrade(&limiters.lease),
            },
        );
        limiters
    }
}

impl TrafficRateLimiters {
    fn new(upload_bps: Option<u64>, download_bps: Option<u64>) -> Self {
        Self {
            lease: Arc::new(TrafficRateLimitLease {
                state: TrafficRateLimitState {
                    upload: upload_bps.map(SharedRateLimiter::new),
                    download: download_bps.map(SharedRateLimiter::new),
                },
                registration: None,
            }),
        }
    }

    fn registered(
        upload_bps: Option<u64>,
        download_bps: Option<u64>,
        registry: Weak<PrincipalRateLimitRegistryInner>,
        key: PrincipalRatePolicyKey,
        generation: u64,
    ) -> Self {
        Self {
            lease: Arc::new(TrafficRateLimitLease {
                state: TrafficRateLimitState {
                    upload: upload_bps.map(SharedRateLimiter::new),
                    download: download_bps.map(SharedRateLimiter::new),
                },
                registration: Some(PrincipalRateLimitRegistration {
                    registry,
                    key,
                    generation,
                }),
            }),
        }
    }

    pub(crate) fn upload(&self) -> Option<SharedRateLimiter> {
        self.lease.state.upload.clone()
    }

    pub(crate) fn download(&self) -> Option<SharedRateLimiter> {
        self.lease.state.download.clone()
    }
}

impl Default for TrafficRateLimiters {
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl Drop for TrafficRateLimitLease {
    fn drop(&mut self) {
        let Some(registration) = self.registration.as_ref() else {
            return;
        };
        let Some(registry) = registration.registry.upgrade() else {
            return;
        };
        let mut entries = registry
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if entries
            .get(&registration.key)
            .is_some_and(|entry| entry.generation == registration.generation)
        {
            entries.remove(&registration.key);
        }
    }
}

fn normalize_rate(rate_bps: Option<u64>) -> Option<u64> {
    rate_bps.filter(|rate_bps| *rate_bps > 0)
}

#[cfg(test)]
mod tests;
