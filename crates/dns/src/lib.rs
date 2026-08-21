//! DNS subsystem — configurable resolver, caching, and routing.
//!
//! When no DNS configuration is provided, `DnsSystem` degrades to the
//! system resolver via `TokioResolver`, preserving existing behavior
//! with zero additional allocation.

mod backends;
mod cache;
mod fake_ip;
mod router;
mod system;
pub mod udp; // DNS wire helpers (build_dns_response, etc.) always available

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::Arc;

use zero_config::DnsConfig;
use zero_traits::{DnsResolver, IpAddress};

use backends::ResolverBackend;
use cache::DnsCache;
use fake_ip::FakeIpAllocator;
use router::DnsDispatcher;
use system::TokioSystemResolver;

/// The configured DNS subsystem.
///
/// Implements [`DnsResolver`] so it can be passed directly to
/// `DirectConnector` and all upstream handlers.
///
/// Inner state is under a read-write lock so DNS config can be
/// hot-reloaded without restarting the proxy.
pub struct DnsSystem {
    inner: std::sync::RwLock<DnsSystemInner>,
    egress_interface: zero_platform_tokio::EgressInterfaceControl,
}

impl fmt::Debug for DnsSystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self.inner.read().expect("dns system lock poisoned") {
            DnsSystemInner::System(_) => f.debug_tuple("System").finish(),
            DnsSystemInner::Configured { servers, .. } => f
                .debug_struct("Configured")
                .field("servers", &servers.len())
                .finish(),
        }
    }
}

enum DnsSystemInner {
    /// No DNS config supplied — passthrough to the system resolver.
    System(TokioSystemResolver),
    /// Fully configured with servers, routing, cache, and optional fake IP.
    Configured {
        servers: BTreeMap<String, Arc<ResolverBackend>>,
        dispatcher: DnsDispatcher,
        cache: Option<DnsCache>,
        fake_ip: Option<Arc<FakeIpAllocator>>,
    },
}

/// Snapshot of the fields needed for an async `resolve()` call.
/// Extracted from the lock so we don't hold it across await points.
struct ResolveSnapshot {
    servers: BTreeMap<String, Arc<ResolverBackend>>,
    dispatcher: DnsDispatcher,
    cache: Option<DnsCache>,
    fake_ip: Option<Arc<FakeIpAllocator>>,
}

impl DnsSystem {
    /// Build a `DnsSystem` from optional config.
    pub fn build(config: Option<&DnsConfig>) -> io::Result<Self> {
        Self::build_with_egress(
            config,
            zero_platform_tokio::EgressInterfaceControl::default(),
        )
    }

    /// Build a DNS system whose owned sockets follow the shared physical
    /// egress selected by the TUN route transaction.
    pub fn build_with_egress(
        config: Option<&DnsConfig>,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        let dispatch = compile_standalone_dispatch(config)?;
        Self::build_with_egress_and_dispatch(config, dispatch, egress_interface)
    }

    pub fn build_with_egress_and_dispatch(
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        Ok(Self {
            inner: std::sync::RwLock::new(Self::build_inner(config, dispatch, &egress_interface)?),
            egress_interface,
        })
    }

    fn build_inner(
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
        egress_interface: &zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<DnsSystemInner> {
        let Some(cfg) = config else {
            return Ok(DnsSystemInner::System(TokioSystemResolver));
        };

        let mut servers = BTreeMap::new();
        for (tag, server) in &cfg.servers {
            servers.insert(
                tag.clone(),
                Arc::new(ResolverBackend::build(server, egress_interface.clone())?),
            );
        }

        let dispatcher = DnsDispatcher::new(dispatch.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing compiled DNS dispatch")
        })?);
        let cache = cfg.cache.as_ref().map(DnsCache::new);
        let fake_ip = cfg
            .fake_ip()
            .map(FakeIpAllocator::new)
            .transpose()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            .map(Arc::new);

        Ok(DnsSystemInner::Configured {
            servers,
            dispatcher,
            cache,
            fake_ip,
        })
    }

    /// Hot-reload DNS configuration.
    ///
    /// In-flight resolutions continue using the old inner state until they
    /// complete; new resolutions see the updated config immediately.
    pub fn reload(&self, config: Option<&DnsConfig>) -> io::Result<()> {
        let dispatch = compile_standalone_dispatch(config)?;
        self.reload_with_dispatch(config, dispatch)
    }

    pub fn reload_with_dispatch(
        &self,
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
    ) -> io::Result<()> {
        let new_inner = Self::build_inner(config, dispatch, &self.egress_interface)?;
        let mut guard = self.inner.write().expect("dns system lock poisoned");
        *guard = new_inner;
        Ok(())
    }

    /// Reverse lookup: fake IP → real domain.
    /// Used before route_decision to restore the original target domain.
    pub async fn lookup_fake_ip(&self, ip: &IpAddress) -> Option<String> {
        let fake_ip = {
            let guard = self.inner.read().expect("dns system lock poisoned");
            match &*guard {
                DnsSystemInner::Configured {
                    fake_ip: Some(alloc),
                    ..
                } => Some(Arc::clone(alloc)),
                _ => None,
            }
        };
        match fake_ip {
            Some(alloc) => alloc.lookup(ip).await,
            None => None,
        }
    }

    /// Whether a DNS cache is configured.
    pub fn cache_enabled(&self) -> bool {
        let guard = self.inner.read().expect("dns system lock poisoned");
        matches!(&*guard, DnsSystemInner::Configured { cache: Some(_), .. })
    }

    /// Whether fake-IP allocation is configured.
    pub fn fake_ip_enabled(&self) -> bool {
        let guard = self.inner.read().expect("dns system lock poisoned");
        matches!(
            &*guard,
            DnsSystemInner::Configured {
                fake_ip: Some(_),
                ..
            }
        )
    }

    /// Return the first TUN-owned address that falls inside the active
    /// synthetic pool. Used by both managed and command-driven TUN startup.
    pub fn fake_ip_conflict(&self, addresses: &[std::net::IpAddr]) -> Option<std::net::IpAddr> {
        let allocator = self.snapshot_fake_ip()?;
        addresses
            .iter()
            .copied()
            .find(|address| allocator.contains(*address))
    }

    /// Inspect a cached domain (diagnostic). Returns (addresses, seconds to
    /// expiry). `None` if cache disabled, miss, or expired.
    pub async fn inspect_cache(&self, domain: &str) -> Option<(Vec<String>, u64)> {
        let cache = self.snapshot_cache()?;
        let (ips, ttl) = cache.inspect(domain).await?;
        Some((ips.iter().map(format_ip_address).collect(), ttl))
    }

    /// Snapshot live cache entries (diagnostic), capped to `limit`.
    pub async fn list_cache(&self, limit: usize) -> Vec<(String, Vec<String>, u64)> {
        let Some(cache) = self.snapshot_cache() else {
            return Vec::new();
        };
        cache
            .entries(limit)
            .await
            .into_iter()
            .map(|(domain, ips, ttl)| (domain, ips.iter().map(format_ip_address).collect(), ttl))
            .collect()
    }

    /// Forward fake-IP lookup (diagnostic): domain → assigned fake IP, without
    /// allocating. Returns the formatted IP, or `None` if fake IP is disabled
    /// or the domain has no mapping.
    pub async fn lookup_fake_ip_domain(&self, domain: &str) -> Option<String> {
        let alloc = self.snapshot_fake_ip()?;
        let ip = alloc.lookup_domain(domain).await?;
        Some(format_ip_address(&ip))
    }

    fn snapshot_cache(&self) -> Option<DnsCache> {
        let guard = self.inner.read().expect("dns system lock poisoned");
        match &*guard {
            DnsSystemInner::Configured { cache: Some(c), .. } => Some(c.clone()),
            _ => None,
        }
    }

    fn snapshot_fake_ip(&self) -> Option<Arc<FakeIpAllocator>> {
        let guard = self.inner.read().expect("dns system lock poisoned");
        match &*guard {
            DnsSystemInner::Configured {
                fake_ip: Some(a), ..
            } => Some(Arc::clone(a)),
            _ => None,
        }
    }

    /// Take a snapshot of the current inner state for an async resolve.
    fn snapshot(&self) -> Option<ResolveSnapshot> {
        let guard = self.inner.read().expect("dns system lock poisoned");
        match &*guard {
            DnsSystemInner::System(_) => None,
            DnsSystemInner::Configured {
                servers,
                dispatcher,
                cache,
                fake_ip,
            } => Some(ResolveSnapshot {
                servers: servers.clone(),
                dispatcher: dispatcher.clone(),
                cache: cache.clone(),
                fake_ip: fake_ip.clone(),
            }),
        }
    }

    /// Resolve a domain through the configured real DNS backends.
    ///
    /// Unlike [`DnsResolver::resolve`], this never allocates or returns a
    /// synthetic fake IP. Internal routing and upstream dialing use this path
    /// after a fake-IP target has been restored to its original domain.
    pub async fn resolve_real(&self, domain: &str) -> io::Result<Vec<IpAddress>> {
        match self.snapshot() {
            Some(snapshot) => resolve_snapshot(domain, snapshot).await,
            None => self.resolve_system(domain).await,
        }
    }

    /// Answer one raw UDP DNS query through the configured resolver. The
    /// regular resolver path is intentional: when Fake-IP is enabled, A
    /// queries receive the synthetic address consumed by TUN TCP/UDP routing.
    pub async fn answer_udp_query(&self, query: &[u8]) -> io::Result<Vec<u8>> {
        let question = udp::parse_dns_question(query)?;
        let mut addresses = match question.query_type {
            1 | 28 => DnsResolver::resolve(self, &question.domain).await?,
            _ => Vec::new(),
        };
        addresses.retain(|address| {
            matches!(
                (question.query_type, address),
                (1, IpAddress::V4(_)) | (28, IpAddress::V6(_))
            )
        });
        Ok(udp::build_dns_response(query, &addresses))
    }

    async fn resolve_system(&self, domain: &str) -> io::Result<Vec<IpAddress>> {
        let sys_resolver = {
            let guard = self.inner.read().expect("dns system lock poisoned");
            match &*guard {
                DnsSystemInner::System(resolver) => *resolver,
                _ => TokioSystemResolver,
            }
        };
        sys_resolver.resolve(domain).await
    }
}

impl DnsResolver for DnsSystem {
    type Error = io::Error;

    async fn resolve(&self, domain: &str) -> Result<Vec<IpAddress>, Self::Error> {
        let snapshot = match self.snapshot() {
            Some(s) => s,
            None => return self.resolve_system(domain).await,
        };

        // Fake IP path: return synthetic IP instead of real resolution.
        if let Some(alloc) = &snapshot.fake_ip {
            if !alloc.is_excluded(domain) {
                if let Some(ip) = alloc.alloc(domain).await {
                    return Ok(vec![ip]);
                }
            }
        }

        resolve_snapshot(domain, snapshot).await
    }
}

async fn resolve_snapshot(domain: &str, snapshot: ResolveSnapshot) -> io::Result<Vec<IpAddress>> {
    // 1. Check cache.
    if let Some(ref cache) = snapshot.cache {
        if let Some(ips) = cache.get(domain).await {
            return Ok(ips);
        }
    }

    // 2. Dispatch to exactly one backend; there is no implicit fallback.
    let selected = snapshot.dispatcher.select(domain);
    let backend = snapshot.servers.get(selected).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("DNS dispatch selected undefined backend `{selected}`"),
        )
    })?;
    let result = backend.resolve(domain).await;

    // 3. Cache on success (default TTL 300s).
    if let (Some(cache), Ok(ips)) = (&snapshot.cache, &result) {
        cache.put(domain.to_owned(), ips.clone(), 300).await;
    }

    result
}

fn compile_standalone_dispatch(
    config: Option<&DnsConfig>,
) -> io::Result<Option<zero_router::DomainDispatcher<String>>> {
    config
        .map(|config| {
            config
                .compile_dispatch(&BTreeMap::new(), None)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
        })
        .transpose()
}

/// Format a `zero_traits::IpAddress` as a string (diagnostic display).
fn format_ip_address(ip: &IpAddress) -> String {
    match ip {
        IpAddress::V4(octets) => std::net::Ipv4Addr::from(*octets).to_string(),
        IpAddress::V6(octets) => std::net::Ipv6Addr::from(*octets).to_string(),
    }
}
