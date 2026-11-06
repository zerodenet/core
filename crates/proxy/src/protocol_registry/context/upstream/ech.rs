use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use zero_platform_tokio::contract::dns::DnsService;
mod update;
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
            resolver_generation: self.dns.generation().resolver,
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
        self.start_update(key, entry, force, true);
    }
    async fn update(
        &self,
        key: CacheKey,
        entry: Arc<Entry>,
        force: EchForceQuery,
    ) -> io::Result<Option<Vec<u8>>> {
        let pending = self.start_update(key.clone(), entry.clone(), force, false);
        let record = match pending.wait().await {
            Ok(record) => record,
            Err(source) => {
                if force != EchForceQuery::Full
                    && self.egress.generation() == key.egress_generation
                    && self.dns.generation().resolver == key.resolver_generation
                {
                    *entry.record.write().await = Some(CacheRecord {
                        material: None,
                        expires: Instant::now() + DEFAULT_TTL,
                        failure: Some(source.to_string()),
                    });
                }
                return Err(update::query_failure(source));
            }
        };
        if self.egress.generation() != key.egress_generation
            || self.dns.generation().resolver != key.resolver_generation
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "ECH cache result belongs to a retired generation",
            ));
        }
        apply_force(record, force)
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
    resolver_generation: u64,
}

#[derive(Default)]
struct Entry {
    record: RwLock<Option<CacheRecord>>,
    update: Mutex<Option<Arc<update::Update>>>,
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
