//! DNS subsystem — configurable resolver, caching, and routing.
//!
//! When no DNS configuration is provided, `DnsSystem` degrades to the
//! system resolver via `TokioResolver`, preserving existing behavior
//! with zero additional allocation.

mod backends;
mod cache;
mod fake_ip;
mod message;
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
pub use fake_ip::FakeIpStats;
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
#[derive(Clone)]
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
            inner: std::sync::RwLock::new(Self::build_inner(
                config,
                dispatch,
                &egress_interface,
                None,
            )?),
            egress_interface,
        })
    }

    fn build_inner(
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
        egress_interface: &zero_platform_tokio::EgressInterfaceControl,
        previous_fake_ip: Option<Arc<FakeIpAllocator>>,
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
        let fake_ip = match cfg.fake_ip() {
            Some(config)
                if previous_fake_ip
                    .as_ref()
                    .is_some_and(|allocator| allocator.compatible_with(config)) =>
            {
                previous_fake_ip
            }
            Some(config) => {
                Some(Arc::new(FakeIpAllocator::new(config).map_err(|error| {
                    io::Error::new(io::ErrorKind::InvalidInput, error)
                })?))
            }
            None => None,
        };

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
        let previous_fake_ip = self.snapshot_fake_ip();
        let new_inner =
            Self::build_inner(config, dispatch, &self.egress_interface, previous_fake_ip)?;
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
        let conflict = addresses
            .iter()
            .copied()
            .find(|address| allocator.contains(*address));
        if conflict.is_some() {
            allocator.record_collision();
        }
        conflict
    }

    pub fn fake_ip_contains(&self, address: std::net::IpAddr) -> bool {
        self.snapshot_fake_ip()
            .is_some_and(|allocator| allocator.contains(address))
    }

    pub async fn fake_ip_stats(&self) -> Option<FakeIpStats> {
        Some(self.snapshot_fake_ip()?.stats().await)
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
        let mut addresses = Vec::new();
        let mut first_error = None;
        for query_type in [message::TYPE_A, message::TYPE_AAAA] {
            match self.resolve_real_type(domain, query_type).await {
                Ok(mut resolved) => addresses.append(&mut resolved),
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        if addresses.is_empty() {
            if let Some(error) = first_error {
                return Err(error);
            }
        }
        Ok(addresses)
    }

    /// Resolve one address family through real DNS without allocating Fake-IP.
    pub async fn resolve_real_type(
        &self,
        domain: &str,
        query_type: u16,
    ) -> io::Result<Vec<IpAddress>> {
        if !matches!(query_type, message::TYPE_A | message::TYPE_AAAA) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "address resolution only supports A or AAAA",
            ));
        }
        let domain = message::normalize_domain(domain)?;
        match self.snapshot() {
            Some(snapshot) => resolve_snapshot_type(&domain, query_type, snapshot).await,
            None => self.resolve_system_type(&domain, query_type).await,
        }
    }

    /// Answer one DNS datagram. Invalid requests and upstream failures are
    /// converted into FORMERR/SERVFAIL responses so clients do not time out.
    pub async fn answer_udp_query(&self, query: &[u8]) -> io::Result<Vec<u8>> {
        let response = self.answer_query(query).await;
        Ok(message::fit_response_to_udp(query, response))
    }

    /// Answer one DNS-over-TCP message without UDP payload truncation.
    pub async fn answer_tcp_query(&self, query: &[u8]) -> io::Result<Vec<u8>> {
        Ok(self.answer_query(query).await)
    }

    /// Build a bounded explicit failure for a query that cannot be admitted.
    pub fn busy_response(&self, query: &[u8]) -> Vec<u8> {
        message::fit_response_to_udp(
            query,
            message::build_error_response(query, message::RCODE_SERVFAIL, false),
        )
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

    async fn resolve_system_type(
        &self,
        domain: &str,
        query_type: u16,
    ) -> io::Result<Vec<IpAddress>> {
        let resolver = {
            let guard = self.inner.read().expect("dns system lock poisoned");
            match &*guard {
                DnsSystemInner::System(resolver) => *resolver,
                _ => TokioSystemResolver,
            }
        };
        resolver.resolve_type(domain, query_type).await
    }

    async fn answer_query(&self, query: &[u8]) -> Vec<u8> {
        let question = match message::parse_question(query) {
            Ok(question) => question,
            Err(_) => return message::build_error_response(query, message::RCODE_FORMERR, false),
        };
        let snapshot = self.snapshot();
        if let Some(allocator) = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.fake_ip.as_ref())
        {
            if !allocator.is_excluded(&question.domain) {
                if question.query_type == message::TYPE_A {
                    return match allocator.alloc(&question.domain).await {
                        Some(address) => message::build_address_response(
                            query,
                            &[address],
                            allocator.ttl_seconds(),
                        ),
                        None => {
                            message::build_error_response(query, message::RCODE_SERVFAIL, false)
                        }
                    };
                }
                if question.query_type == message::TYPE_AAAA {
                    return message::build_address_response(query, &[], allocator.ttl_seconds());
                }
            }
        }

        if let Some(cache) = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.cache.as_ref())
            .cloned()
        {
            if let Some(cached) = cache
                .get_response(&question.domain, question.query_type, query)
                .await
            {
                return cached;
            }
        }

        let result = match snapshot.as_ref() {
            Some(snapshot) => {
                let selected = snapshot.dispatcher.select(&question.domain);
                match snapshot.servers.get(selected) {
                    Some(backend) => backend.exchange(query).await,
                    None => Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("DNS dispatch selected undefined backend `{selected}`"),
                    )),
                }
            }
            None => {
                backends::ResolverBackend::System(TokioSystemResolver)
                    .exchange(query)
                    .await
            }
        };
        match result.and_then(|response| {
            let parsed = message::parse_response(query, &response)?;
            Ok((response, parsed))
        }) {
            Ok((response, parsed)) => {
                if let (Some(cache), Some(ttl_seconds)) = (
                    snapshot.and_then(|snapshot| snapshot.cache),
                    parsed.min_ttl_seconds,
                ) {
                    cache
                        .put_response(
                            &question.domain,
                            question.query_type,
                            parsed.addresses,
                            query.to_vec(),
                            response.clone(),
                            ttl_seconds,
                        )
                        .await;
                }
                response
            }
            Err(error) => {
                tracing::warn!(%error, domain = %question.domain, "DNS query failed; returning SERVFAIL");
                message::build_error_response(query, message::RCODE_SERVFAIL, false)
            }
        }
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

        let domain = message::normalize_domain(domain)?;
        let mut addresses = Vec::new();
        let mut first_error = None;
        for query_type in [message::TYPE_A, message::TYPE_AAAA] {
            match resolve_snapshot_type(&domain, query_type, snapshot.clone()).await {
                Ok(mut resolved) => addresses.append(&mut resolved),
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        if addresses.is_empty() {
            if let Some(error) = first_error {
                return Err(error);
            }
        }
        Ok(addresses)
    }
}

async fn resolve_snapshot_type(
    domain: &str,
    query_type: u16,
    snapshot: ResolveSnapshot,
) -> io::Result<Vec<IpAddress>> {
    // 1. Check cache.
    if let Some(ref cache) = snapshot.cache {
        if let Some(ips) = cache.get(domain, query_type).await {
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
    let result = backend.resolve_type(domain, query_type).await;

    // 3. Cache on success using the upstream record TTL.
    if let (Some(cache), Ok(resolved)) = (&snapshot.cache, &result) {
        cache
            .put(
                domain,
                query_type,
                resolved.addresses.clone(),
                resolved.ttl_seconds,
            )
            .await;
    }

    result.map(|resolved| resolved.addresses)
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
