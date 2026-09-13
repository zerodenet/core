use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex as AsyncMutex, RwLock};
use zero_traits::EchForceQuery;
use zero_transport::tls::ech::{EchConfigResolveFuture, EchConfigResolver};

const MAX_CACHE_ENTRIES: usize = 256;
const MAX_STALE: Duration = Duration::from_secs(4 * 60 * 60);
const DEFAULT_TTL: Duration = Duration::from_secs(300);

#[derive(Clone)]
pub(super) struct RuntimeEchResolver {
    dns: Arc<zero_dns::DnsSystem>,
    egress: zero_platform_tokio::EgressInterfaceControl,
    cache: Arc<Mutex<Cache>>,
}

impl RuntimeEchResolver {
    pub(super) fn new(
        dns: Arc<zero_dns::DnsSystem>,
        egress: zero_platform_tokio::EgressInterfaceControl,
    ) -> Self {
        Self {
            dns,
            egress,
            cache: Arc::new(Mutex::new(Cache::default())),
        }
    }

    async fn resolve_inner(
        self,
        server: String,
        query_name: String,
        force: EchForceQuery,
    ) -> io::Result<Option<Vec<u8>>> {
        let key = CacheKey {
            server,
            query_name,
            egress_generation: self.egress.generation(),
        };
        let entry = self.entry(key.clone());
        if let Some(record) = entry.record.read().await.clone() {
            let now = Instant::now();
            if record.expires > now {
                if record.failure.is_some() && force != EchForceQuery::None {
                    self.refresh(key, entry.clone(), force);
                }
                return apply_force(record, force);
            }
            if now.duration_since(record.expires) <= MAX_STALE {
                self.refresh(key, entry.clone(), force);
                return apply_force(record, force);
            }
        }
        self.update(key, entry, force).await
    }

    fn entry(&self, key: CacheKey) -> Arc<Entry> {
        let mut cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = cache.entries.get(&key) {
            return entry.clone();
        }
        while cache.entries.len() >= MAX_CACHE_ENTRIES {
            let Some(oldest) = cache.order.pop_front() else {
                break;
            };
            cache.entries.remove(&oldest);
        }
        let entry = Arc::new(Entry::default());
        cache.order.push_back(key.clone());
        cache.entries.insert(key, entry.clone());
        entry
    }

    fn refresh(&self, key: CacheKey, entry: Arc<Entry>, force: EchForceQuery) {
        if entry
            .refreshing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let resolver = self.clone();
        tokio::spawn(async move {
            let _ = resolver.update(key, entry.clone(), force).await;
            entry.refreshing.store(false, Ordering::Release);
        });
    }

    async fn update(
        &self,
        key: CacheKey,
        entry: Arc<Entry>,
        force: EchForceQuery,
    ) -> io::Result<Option<Vec<u8>>> {
        let _update = entry.update.lock().await;
        if let Some(record) = entry.record.read().await.clone() {
            if record.expires > Instant::now() && record.failure.is_none() {
                return apply_force(record, force);
            }
        }
        match self
            .dns
            .query_ech_config(&key.query_name, &key.server)
            .await
        {
            Ok(answer) => {
                let ttl = if answer.ttl_seconds == 0 {
                    DEFAULT_TTL
                } else {
                    Duration::from_secs(u64::from(answer.ttl_seconds))
                };
                let record = CacheRecord {
                    material: answer.config_list,
                    expires: Instant::now() + ttl,
                    failure: None,
                };
                *entry.record.write().await = Some(record.clone());
                apply_force(record, force)
            }
            Err(error) if force == EchForceQuery::Full => Err(query_error(error)),
            Err(error) => {
                let record = CacheRecord {
                    material: None,
                    expires: Instant::now() + DEFAULT_TTL,
                    failure: Some(error.to_string()),
                };
                *entry.record.write().await = Some(record.clone());
                apply_force(record, force)
            }
        }
    }
}

impl EchConfigResolver for RuntimeEchResolver {
    fn resolve(
        &self,
        server: String,
        query_name: String,
        force_query: EchForceQuery,
    ) -> EchConfigResolveFuture {
        let resolver = self.clone();
        Box::pin(async move {
            resolver
                .resolve_inner(server, query_name, force_query)
                .await
        })
    }
}

#[derive(Default)]
struct Cache {
    entries: HashMap<CacheKey, Arc<Entry>>,
    order: VecDeque<CacheKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    server: String,
    query_name: String,
    egress_generation: u64,
}

#[derive(Default)]
struct Entry {
    record: RwLock<Option<CacheRecord>>,
    update: AsyncMutex<()>,
    refreshing: AtomicBool,
}

#[derive(Clone)]
struct CacheRecord {
    material: Option<Vec<u8>>,
    expires: Instant,
    failure: Option<String>,
}

fn apply_force(record: CacheRecord, force: EchForceQuery) -> io::Result<Option<Vec<u8>>> {
    if let Some(error) = record.failure {
        return Err(query_error(error));
    }
    if force == EchForceQuery::Full && record.material.is_none() {
        return Err(force_error());
    }
    Ok(record.material)
}

fn force_error() -> io::Error {
    io::Error::other("required ECH DNS material is unavailable")
}

fn query_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("ECH DNS query failed: {error}"))
}

#[cfg(test)]
mod tests;
