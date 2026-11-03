//! Admission belongs to the same real carrier pool; views retain no live owner.
use super::{failure, ManagedPool};
use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash, Hasher},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{network::UpstreamBinding, ChannelError, Reason, Stage};

#[derive(Default)]
pub(super) struct Registry {
    scopes: Mutex<HashMap<Arc<str>, Arc<Scope>>>,
    legacy_used: AtomicBool,
    nonce: Mutex<Option<u64>>,
}
#[derive(Default)]
pub(super) struct Scope(Mutex<Mode>);
#[derive(Default)]
enum Mode {
    #[default]
    Cold,
    Legacy,
    Owned(Weak<ManagedPool>),
    Retiring,
}
#[derive(Clone, Default)]
pub struct PoolControl {
    pub(super) registry: Arc<Registry>,
    pub(super) key: Option<Arc<str>>,
    pub(super) scope: Arc<Scope>,
}
impl PoolControl {
    pub fn refresh(&self) -> Result<Self, crate::RuntimeError> {
        let key = self
            .key
            .clone()
            .ok_or_else(|| std::io::Error::other("native pool view is unscoped"))?;
        Ok(self.scoped(key))
    }
    pub fn scoped(&self, key: Arc<str>) -> Self {
        let mut scopes = self.registry.scopes.lock().unwrap();
        scopes.retain(|_, value| {
            Arc::strong_count(value) > 1 || !matches!(*value.0.lock().unwrap(), Mode::Cold)
        });
        let current = scopes.entry(key.clone()).or_default().clone();
        Self {
            registry: self.registry.clone(),
            key: Some(key),
            scope: current,
        }
    }
    pub fn enter_legacy(&self) -> Result<(), crate::RuntimeError> {
        let scopes = self.registry.scopes.lock().unwrap();
        // Keys name disjoint entries in the same actual protocol map. A native
        // owner of one key must not disable a different legacy outbound.
        if self.key.is_none()
            && scopes
                .values()
                .any(|scope| matches!(*scope.0.lock().unwrap(), Mode::Owned(_) | Mode::Retiring))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "unscoped carrier pool overlaps a native owner",
            )
            .into());
        }
        let mut scope = self.scope.0.lock().unwrap();
        if !matches!(*scope, Mode::Cold | Mode::Legacy) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "carrier pool is owned by a native generation",
            )
            .into());
        }
        *scope = Mode::Legacy;
        if self.key.is_none() {
            self.registry.legacy_used.store(true, Ordering::Release);
        }
        Ok(())
    }
    /// Cold recipes may describe an existing native generation for host reuse.
    /// Prior legacy exposure is sticky, even after every old view is dropped.
    pub fn native_eligible(&self) -> bool {
        !self.registry.legacy_used.load(Ordering::Acquire)
            && !matches!(*self.scope.0.lock().unwrap(), Mode::Legacy | Mode::Retiring)
    }
    /// Compatibility execution can delegate to the installed actual owner.
    /// A retired view must fail rather than spawn another legacy driver.
    pub fn installed(&self) -> Result<Option<Arc<ManagedPool>>, crate::RuntimeError> {
        let scope = self.scope.0.lock().unwrap();
        match &*scope {
            Mode::Cold | Mode::Legacy => Ok(None),
            Mode::Owned(owner) => owner
                .upgrade()
                .filter(|owner| owner.ready())
                .map(Some)
                .ok_or_else(|| std::io::Error::other("native carrier pool is unavailable").into()),
            Mode::Retiring => Err(std::io::Error::other("native carrier pool is retired").into()),
        }
    }
    pub fn managed(&self) -> Result<Arc<ManagedPool>, crate::RuntimeError> {
        let scope = self.scope.0.lock().unwrap();
        if let Mode::Owned(owner) = &*scope {
            if let Some(owner) = owner.upgrade().filter(|owner| owner.ready()) {
                return Ok(owner);
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "native carrier pool generation is unavailable",
        )
        .into())
    }
    /// Protocol-owned complete profile equality is opaque here. It is never
    /// printed, interpreted as config, or used as native closure evidence.
    pub fn options(
        &self,
        profile: Arc<str>,
        endpoint: Destination,
        binding: UpstreamBinding,
        limit: NonZeroUsize,
        open_budget: Duration,
    ) -> Result<PoolOptions, crate::RuntimeError> {
        let invalid = || {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "native carrier pool requires a scoped literal root and finite budget",
            )
        };
        if self.key.is_none()
            || !self.native_eligible()
            || matches!(endpoint.address, zero_core::Address::Domain(_))
            || endpoint.port == 0
            || !binding.prefix.is_empty()
            || open_budget.is_zero()
            || tokio::time::Instant::now()
                .checked_add(open_budget)
                .is_none()
        {
            return Err(invalid().into());
        }
        binding.dial.validate().map_err(|_| invalid())?;
        // Different actual adapter pools are never canonicalized as one owner,
        // including after old weak allocation addresses have been reused.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = {
            let mut identity = self.registry.nonce.lock().unwrap();
            if let Some(actual) = *identity {
                actual
            } else {
                let mut current = NEXT.load(Ordering::Acquire);
                let next = loop {
                    let next = current.checked_add(1).ok_or_else(invalid)?;
                    match NEXT.compare_exchange_weak(
                        current,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break next,
                        Err(observed) => current = observed,
                    }
                };
                *identity = Some(next);
                next
            }
        };
        // Keyed hints stay secret-free. The complete immutable description is
        // retained below for real equality before any acquisition.
        static HASHERS: std::sync::OnceLock<[std::collections::hash_map::RandomState; 2]> =
            std::sync::OnceLock::new();
        let mut identity = Vec::new();
        for keys in HASHERS.get_or_init(|| [Default::default(), Default::default()]) {
            let mut hash = keys.build_hasher();
            nonce.hash(&mut hash);
            self.key.hash(&mut hash);
            profile.hash(&mut hash);
            endpoint.hash(&mut hash);
            binding.hash(&mut hash);
            limit.hash(&mut hash);
            open_budget.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(PoolOptions {
            registry: Arc::downgrade(&self.registry),
            key: self.key.clone().unwrap(),
            profile,
            endpoint,
            binding,
            limit,
            open_budget,
            identity: identity.into(),
            on_closed: None,
        })
    }
}
pub struct PoolOptions {
    pub(super) registry: Weak<Registry>,
    pub(super) key: Arc<str>,
    pub(super) profile: Arc<str>,
    pub(super) endpoint: Destination,
    pub(super) binding: UpstreamBinding,
    pub(super) limit: NonZeroUsize,
    pub(super) open_budget: Duration,
    pub(super) identity: Arc<[u8]>,
    pub(super) on_closed:
        Option<Arc<dyn Fn(zero_core::component::ComponentInstanceId) + Send + Sync>>,
}
impl std::fmt::Debug for PoolOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeCarrierPoolOptions")
            .field("endpoint", &self.endpoint)
            .field("carrier_limit", &self.limit)
            .finish_non_exhaustive()
    }
}
impl PoolOptions {
    /// Protocol-private cache reclamation runs only after actual closure. The
    /// callback grants no I/O authority and supplies no closure evidence.
    pub fn on_confirmed_close(
        mut self,
        reclaim: impl Fn(zero_core::component::ComponentInstanceId) + Send + Sync + 'static,
    ) -> Self {
        self.on_closed = Some(Arc::new(reclaim));
        self
    }
    pub fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.binding
    }
}
impl Registry {
    pub(super) fn claim(
        &self,
        key: &Arc<str>,
        owner: &Arc<ManagedPool>,
    ) -> Result<Arc<Scope>, ChannelError> {
        let mut scopes = self.scopes.lock().unwrap();
        if self.legacy_used.load(Ordering::Acquire) {
            return Err(failure(owner.instance, Stage::Prepare, Reason::StalePlan));
        }
        let current = scopes.entry(key.clone()).or_default();
        let scope = current.clone();
        {
            let mut mode = scope.0.lock().unwrap();
            if !matches!(*mode, Mode::Cold) {
                return Err(failure(owner.instance, Stage::Prepare, Reason::StalePlan));
            }
            *mode = Mode::Owned(Arc::downgrade(owner));
        }
        Ok(scope)
    }
    pub(super) fn confirm(&self, key: &Arc<str>, actual: &Arc<Scope>) {
        *actual.0.lock().unwrap() = Mode::Retiring;
        let mut scopes = self.scopes.lock().unwrap();
        if scopes
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, actual))
        {
            scopes.remove(key);
        }
    }
}

#[cfg(test)]
#[path = "../../tests/pooled_driver/control.rs"]
mod tests;
