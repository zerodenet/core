//! DNS subsystem — configurable resolver, caching, and routing.
//!
//! When no DNS configuration is provided, `DnsSystem` degrades to the
//! system resolver via `TokioResolver`, preserving existing behavior
//! with zero additional allocation.

mod backends;
mod cache;
mod fake_ip;
mod message;
mod reverse;
mod router;
mod system;
pub mod udp; // DNS wire helpers (build_dns_response, etc.) always available

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use zero_config::{DnsAddressFamilyPolicy, DnsConfig, DnsPolicyConfig};
use zero_traits::{DnsResolver, IpAddress};

use backends::ResolverBackend;
use cache::{DnsCache, DnsWireCacheValue};
use fake_ip::FakeIpAllocator;
pub use fake_ip::{default_fake_ip_state_path, FakeIpClearResult, FakeIpClearTarget, FakeIpStats};
use reverse::RealIpReverseIndex;
pub use reverse::RealIpReverseLookup;
use router::DnsDispatcher;
use system::TokioSystemResolver;

/// Isolation domain for DNS queries emitted by the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DnsQueryRole {
    /// Client/intercepted queries and proxy-routed target resolution.
    Default,
    /// Targets selected for the direct outbound.
    Direct,
    /// Proxy nodes and their carrier endpoints.
    Node,
}

/// One backend attempt retained for DNS diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsQueryAttempt {
    pub domain: String,
    pub role: DnsQueryRole,
    pub server_tag: String,
    pub transport: &'static str,
    pub server_endpoints: Vec<String>,
    pub outbound: String,
    pub success: bool,
    pub failure_reason: Option<String>,
}

/// Future returned by a runtime-provided DNS TCP detour connector.
pub type DnsOutboundConnectFuture =
    Pin<Box<dyn Future<Output = io::Result<zero_platform_tokio::TcpRelayStream>> + Send + 'static>>;

/// Opens a TCP stream to a deterministic DNS endpoint through a named route
/// target. The proxy runtime supplies this bridge; standalone DNS users get a
/// clear error if they configure a detour without installing one.
pub trait DnsOutboundConnector: fmt::Debug + Send + Sync {
    fn connect(&self, outbound: String, endpoint: SocketAddr) -> DnsOutboundConnectFuture;
}

impl DnsQueryRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Direct => "direct",
            Self::Node => "node",
        }
    }
}

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
    fake_ip_state_path: Option<PathBuf>,
    fake_ip_state_lease: std::sync::Mutex<Option<Arc<fake_ip::StateLease>>>,
    query_attempts: Arc<std::sync::Mutex<VecDeque<DnsQueryAttempt>>>,
    outbound_connector: std::sync::RwLock<Option<Arc<dyn DnsOutboundConnector>>>,
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
        servers: BTreeMap<String, ResolverServer>,
        dispatcher: DnsDispatcher,
        cache: Option<DnsCache>,
        fake_ip: Option<Arc<FakeIpAllocator>>,
        reverse_mapping: Option<RealIpReverseIndex>,
        policy: Box<DnsPolicyConfig>,
    },
}

#[derive(Clone)]
struct ResolverServer {
    backend: Arc<ResolverBackend>,
    detour: Option<String>,
}

/// Snapshot of the fields needed for an async `resolve()` call.
/// Extracted from the lock so we don't hold it across await points.
#[derive(Clone)]
struct ResolveSnapshot {
    servers: BTreeMap<String, ResolverServer>,
    dispatcher: DnsDispatcher,
    cache: Option<DnsCache>,
    fake_ip: Option<Arc<FakeIpAllocator>>,
    reverse_mapping: Option<RealIpReverseIndex>,
    policy: DnsPolicyConfig,
    query_attempts: Arc<std::sync::Mutex<VecDeque<DnsQueryAttempt>>>,
    outbound_connector: Option<Arc<dyn DnsOutboundConnector>>,
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
        Self::build_with_egress_dispatch_and_state(config, dispatch, egress_interface, None)
    }

    /// Build DNS with the runtime-owned Fake-IP persistence path.
    ///
    /// Library-only callers remain in-memory by default. The proxy runtime
    /// supplies a path derived from the source configuration so mappings can
    /// survive process restarts without changing the public JSON contract.
    pub fn build_with_egress_dispatch_and_state(
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
        fake_ip_state_path: Option<PathBuf>,
    ) -> io::Result<Self> {
        let fake_ip_state_lease = if config.and_then(DnsConfig::fake_ip).is_some() {
            fake_ip_state_path
                .as_ref()
                .map(|path| fake_ip::StateLease::acquire(path.clone()))
                .transpose()?
        } else {
            None
        };
        let inner = Self::build_inner(
            config,
            dispatch,
            &egress_interface,
            None,
            None,
            fake_ip_state_lease.clone(),
        )?;
        Ok(Self {
            inner: std::sync::RwLock::new(inner),
            egress_interface,
            fake_ip_state_path,
            fake_ip_state_lease: std::sync::Mutex::new(fake_ip_state_lease),
            query_attempts: Arc::new(std::sync::Mutex::new(VecDeque::new())),
            outbound_connector: std::sync::RwLock::new(None),
        })
    }

    fn build_inner(
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
        egress_interface: &zero_platform_tokio::EgressInterfaceControl,
        previous_fake_ip: Option<Arc<FakeIpAllocator>>,
        previous_reverse_mapping: Option<RealIpReverseIndex>,
        fake_ip_state_lease: Option<Arc<fake_ip::StateLease>>,
    ) -> io::Result<DnsSystemInner> {
        let Some(cfg) = config else {
            return Ok(DnsSystemInner::System(TokioSystemResolver));
        };

        let mut servers = BTreeMap::new();
        for (tag, server) in &cfg.servers {
            servers.insert(
                tag.clone(),
                ResolverServer {
                    backend: Arc::new(ResolverBackend::build(server, egress_interface.clone())?),
                    detour: server.detour().map(ToOwned::to_owned),
                },
            );
        }

        let dispatcher = DnsDispatcher::new(dispatch.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing compiled DNS dispatch")
        })?);
        let cache = cfg.cache.as_ref().map(DnsCache::new);
        let reverse_mapping = match &cfg.reverse_mapping {
            Some(config)
                if previous_reverse_mapping
                    .as_ref()
                    .is_some_and(|index| index.compatible_with(config)) =>
            {
                previous_reverse_mapping
            }
            Some(config) => Some(RealIpReverseIndex::new(config)),
            None => None,
        };
        let fake_ip = match cfg.fake_ip() {
            Some(config)
                if previous_fake_ip
                    .as_ref()
                    .is_some_and(|allocator| allocator.compatible_with(config)) =>
            {
                previous_fake_ip
            }
            Some(config) => Some(Arc::new(FakeIpAllocator::new_with_state(
                config,
                fake_ip_state_lease,
            )?)),
            None => None,
        };

        Ok(DnsSystemInner::Configured {
            servers,
            dispatcher,
            cache,
            fake_ip,
            reverse_mapping,
            policy: Box::new(cfg.policy.clone()),
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
        let previous_reverse_mapping = self.snapshot_reverse_mapping();
        let fake_ip_state_lease = if config.and_then(DnsConfig::fake_ip).is_some() {
            let mut guard = self
                .fake_ip_state_lease
                .lock()
                .expect("Fake-IP state lease lock poisoned");
            if guard.is_none() {
                *guard = self
                    .fake_ip_state_path
                    .as_ref()
                    .map(|path| fake_ip::StateLease::acquire(path.clone()))
                    .transpose()?;
            }
            guard.clone()
        } else {
            None
        };
        let new_inner = Self::build_inner(
            config,
            dispatch,
            &self.egress_interface,
            previous_fake_ip,
            previous_reverse_mapping,
            fake_ip_state_lease,
        )?;
        let mut guard = self.inner.write().expect("dns system lock poisoned");
        *guard = new_inner;
        Ok(())
    }

    /// Install the runtime bridge used by DNS servers that specify a detour.
    pub fn set_outbound_connector(&self, connector: Arc<dyn DnsOutboundConnector>) {
        *self
            .outbound_connector
            .write()
            .expect("DNS outbound connector lock poisoned") = Some(connector);
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

    /// Reverse lookup a real DNS answer for transparent traffic recovery.
    /// Shared addresses with multiple live domain candidates are never guessed.
    pub async fn lookup_real_ip(&self, ip: &IpAddress) -> RealIpReverseLookup {
        let Some(index) = self.snapshot_reverse_mapping() else {
            return RealIpReverseLookup::Missing;
        };
        index.lookup(*ip).await
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

    /// Clear all Fake-IP mappings, or one mapping selected by domain/address.
    /// The allocator updates its persistent journal before reporting success.
    pub async fn clear_fake_ip(
        &self,
        target: FakeIpClearTarget,
    ) -> io::Result<Option<FakeIpClearResult>> {
        let Some(allocator) = self.snapshot_fake_ip() else {
            return Ok(None);
        };
        allocator.clear(target).await.map(Some)
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

    fn snapshot_reverse_mapping(&self) -> Option<RealIpReverseIndex> {
        let guard = self.inner.read().expect("dns system lock poisoned");
        match &*guard {
            DnsSystemInner::Configured {
                reverse_mapping: Some(index),
                ..
            } => Some(index.clone()),
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
                reverse_mapping,
                policy,
            } => Some(ResolveSnapshot {
                servers: servers.clone(),
                dispatcher: dispatcher.clone(),
                cache: cache.clone(),
                fake_ip: fake_ip.clone(),
                reverse_mapping: reverse_mapping.clone(),
                policy: policy.as_ref().clone(),
                query_attempts: self.query_attempts.clone(),
                outbound_connector: self
                    .outbound_connector
                    .read()
                    .expect("DNS outbound connector lock poisoned")
                    .clone(),
            }),
        }
    }

    /// Return the newest backend attempts for one normalized query role.
    pub fn recent_query_attempts(
        &self,
        domain: &str,
        role: DnsQueryRole,
        limit: usize,
    ) -> Vec<DnsQueryAttempt> {
        let Ok(domain) = message::normalize_domain(domain) else {
            return Vec::new();
        };
        self.query_attempts
            .lock()
            .expect("DNS query attempt lock poisoned")
            .iter()
            .rev()
            .filter(|attempt| attempt.domain == domain && attempt.role == role)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Resolve a domain through the configured real DNS backends.
    ///
    /// Unlike [`DnsResolver::resolve`], this never allocates or returns a
    /// synthetic fake IP. Internal routing and upstream dialing use this path
    /// after a fake-IP target has been restored to its original domain.
    pub async fn resolve_real(&self, domain: &str) -> io::Result<Vec<IpAddress>> {
        self.resolve_real_for_role(domain, DnsQueryRole::Default)
            .await
    }

    /// Resolve a target selected for the direct outbound through its isolated
    /// DNS role. This never allocates a synthetic Fake-IP.
    pub async fn resolve_direct(&self, domain: &str) -> io::Result<Vec<IpAddress>> {
        self.resolve_real_for_role(domain, DnsQueryRole::Direct)
            .await
    }

    /// Return the address-family policy applied to real direct-target
    /// resolution. The system-resolver fallback preserves Zero's historical
    /// IPv4-first dual-stack behavior.
    pub fn address_family_policy(&self) -> DnsAddressFamilyPolicy {
        self.snapshot()
            .map(|snapshot| snapshot.policy.address_family)
            .unwrap_or_default()
    }

    /// Resolve a proxy node or carrier endpoint through the bootstrap/node
    /// DNS role. This never allocates a synthetic Fake-IP.
    pub async fn resolve_node(&self, domain: &str) -> io::Result<Vec<IpAddress>> {
        self.resolve_real_for_role(domain, DnsQueryRole::Node).await
    }

    async fn resolve_real_for_role(
        &self,
        domain: &str,
        role: DnsQueryRole,
    ) -> io::Result<Vec<IpAddress>> {
        let domain = message::normalize_domain(domain)?;
        match self.snapshot() {
            Some(snapshot) => resolve_snapshot(&domain, role, snapshot).await,
            None => {
                let (ipv4, ipv6) = tokio::join!(
                    self.resolve_system_type(&domain, message::TYPE_A),
                    self.resolve_system_type(&domain, message::TYPE_AAAA),
                );
                combine_address_families(ipv4, ipv6)
            }
        }
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
            Some(snapshot) => {
                resolve_snapshot_type(&domain, query_type, DnsQueryRole::Default, snapshot).await
            }
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
                    return match allocator.alloc_ipv4(&question.domain).await {
                        Ok(Some(address)) => message::build_address_response(
                            query,
                            &[address],
                            allocator.ttl_seconds(),
                        ),
                        Ok(None) => {
                            message::build_error_response(query, message::RCODE_SERVFAIL, false)
                        }
                        Err(error) => {
                            tracing::error!(
                                %error,
                                domain = %question.domain,
                                "failed to persist Fake-IP mapping; returning SERVFAIL"
                            );
                            message::build_error_response(query, message::RCODE_SERVFAIL, false)
                        }
                    };
                }
                if question.query_type == message::TYPE_AAAA {
                    return match allocator.alloc_ipv6(&question.domain).await {
                        Ok(Some(address)) => message::build_address_response(
                            query,
                            &[address],
                            allocator.ttl_seconds(),
                        ),
                        Ok(None) => {
                            message::build_address_response(query, &[], allocator.ttl_seconds())
                        }
                        Err(error) => {
                            tracing::error!(
                                %error,
                                domain = %question.domain,
                                "failed to persist IPv6 Fake-IP mapping; returning SERVFAIL"
                            );
                            message::build_error_response(query, message::RCODE_SERVFAIL, false)
                        }
                    };
                }
            }
        }

        if let Some(cache) = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.cache.as_ref())
            .cloned()
        {
            if let Some(cached) = cache
                .get_response(
                    DnsQueryRole::Default,
                    &question.domain,
                    question.query_type,
                    query,
                )
                .await
            {
                return cached;
            }
        }

        let result = match snapshot.as_ref() {
            Some(snapshot) => {
                exchange_snapshot(query, &question.domain, DnsQueryRole::Default, snapshot)
                    .await
                    .map(|(response, _)| response)
            }
            None => {
                backends::ResolverBackend::System(TokioSystemResolver)
                    .exchange(query, None, None)
                    .await
            }
        };
        match result.and_then(|response| {
            let parsed = message::parse_response(query, &response)?;
            Ok((response, parsed))
        }) {
            Ok((response, parsed)) => {
                if let Some(ttl_seconds) = parsed.min_ttl_seconds {
                    if let Some(reverse_mapping) = snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.reverse_mapping.as_ref())
                    {
                        reverse_mapping
                            .record(&question.domain, &parsed.addresses, ttl_seconds)
                            .await;
                    }
                    if let Some(cache) = snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.cache.as_ref())
                    {
                        cache
                            .put_response(
                                DnsQueryRole::Default,
                                &question.domain,
                                question.query_type,
                                DnsWireCacheValue {
                                    addresses: parsed.addresses.clone(),
                                    query: query.to_vec(),
                                    response: response.clone(),
                                    ttl_seconds,
                                },
                            )
                            .await;
                    }
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
                let addresses =
                    allocate_fake_addresses(alloc, domain, snapshot.policy.address_family).await?;
                if !addresses.is_empty() {
                    return Ok(addresses);
                }
            }
        }

        let domain = message::normalize_domain(domain)?;
        resolve_snapshot(&domain, DnsQueryRole::Default, snapshot).await
    }
}

async fn allocate_fake_addresses(
    allocator: &FakeIpAllocator,
    domain: &str,
    policy: DnsAddressFamilyPolicy,
) -> io::Result<Vec<IpAddress>> {
    match policy {
        DnsAddressFamilyPolicy::Ipv4Only => {
            Ok(allocator.alloc_ipv4(domain).await?.into_iter().collect())
        }
        DnsAddressFamilyPolicy::Ipv6Only => {
            Ok(allocator.alloc_ipv6(domain).await?.into_iter().collect())
        }
        DnsAddressFamilyPolicy::PreferIpv4 | DnsAddressFamilyPolicy::PreferIpv6 => {
            let (ipv4, ipv6) =
                tokio::join!(allocator.alloc_ipv4(domain), allocator.alloc_ipv6(domain),);
            let ipv4 = ipv4?;
            let ipv6 = ipv6?;
            let mut addresses = Vec::with_capacity(2);
            let ordered = if policy == DnsAddressFamilyPolicy::PreferIpv4 {
                [ipv4, ipv6]
            } else {
                [ipv6, ipv4]
            };
            addresses.extend(ordered.into_iter().flatten());
            Ok(addresses)
        }
    }
}

async fn resolve_snapshot(
    domain: &str,
    role: DnsQueryRole,
    snapshot: ResolveSnapshot,
) -> io::Result<Vec<IpAddress>> {
    match snapshot.policy.address_family {
        DnsAddressFamilyPolicy::Ipv4Only => {
            resolve_snapshot_type(domain, message::TYPE_A, role, snapshot).await
        }
        DnsAddressFamilyPolicy::Ipv6Only => {
            resolve_snapshot_type(domain, message::TYPE_AAAA, role, snapshot).await
        }
        DnsAddressFamilyPolicy::PreferIpv4 => {
            let (ipv4, ipv6) = tokio::join!(
                resolve_snapshot_type(domain, message::TYPE_A, role, snapshot.clone()),
                resolve_snapshot_type(domain, message::TYPE_AAAA, role, snapshot),
            );
            combine_address_families(ipv4, ipv6)
        }
        DnsAddressFamilyPolicy::PreferIpv6 => {
            let (ipv4, ipv6) = tokio::join!(
                resolve_snapshot_type(domain, message::TYPE_A, role, snapshot.clone()),
                resolve_snapshot_type(domain, message::TYPE_AAAA, role, snapshot),
            );
            combine_address_families(ipv6, ipv4)
        }
    }
}

fn combine_address_families(
    ipv4: io::Result<Vec<IpAddress>>,
    ipv6: io::Result<Vec<IpAddress>>,
) -> io::Result<Vec<IpAddress>> {
    let mut addresses = Vec::new();
    let mut first_error = None;
    for result in [ipv4, ipv6] {
        match result {
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

async fn resolve_snapshot_type(
    domain: &str,
    query_type: u16,
    role: DnsQueryRole,
    snapshot: ResolveSnapshot,
) -> io::Result<Vec<IpAddress>> {
    // 1. Check cache.
    if let Some(ref cache) = snapshot.cache {
        if let Some(ips) = cache.get(role, domain, query_type).await {
            return Ok(ips);
        }
    }

    // 2. Dispatch to the selected backend, then walk the explicit fallback
    // chain on transport failure, timeout, malformed data, or retryable RCODE.
    let query = message::build_query(domain, query_type)?;
    let result = exchange_snapshot(&query, domain, role, &snapshot)
        .await
        .and_then(|(_, parsed)| match parsed.response_code {
            message::RCODE_NOERROR => Ok(backends::ResolvedAddresses {
                addresses: parsed.addresses,
                ttl_seconds: parsed
                    .min_ttl_seconds
                    .unwrap_or(message::DEFAULT_NEGATIVE_TTL_SECONDS),
            }),
            message::RCODE_NXDOMAIN => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("DNS name `{domain}` does not exist"),
            )),
            code => Err(io::Error::other(format!(
                "DNS server returned response code {code} for `{domain}`"
            ))),
        });

    // 3. Cache on success using the upstream record TTL.
    if let Ok(resolved) = &result {
        if role == DnsQueryRole::Default {
            if let Some(reverse_mapping) = &snapshot.reverse_mapping {
                reverse_mapping
                    .record(domain, &resolved.addresses, resolved.ttl_seconds)
                    .await;
            }
        }
        if let Some(cache) = &snapshot.cache {
            cache
                .put(
                    role,
                    domain,
                    query_type,
                    resolved.addresses.clone(),
                    resolved.ttl_seconds,
                )
                .await;
        }
    }

    result.map(|resolved| resolved.addresses)
}

async fn exchange_snapshot(
    query: &[u8],
    domain: &str,
    role: DnsQueryRole,
    snapshot: &ResolveSnapshot,
) -> io::Result<(Vec<u8>, message::ParsedDnsResponse)> {
    let (selected, fallbacks) = match role {
        DnsQueryRole::Default => (
            snapshot.dispatcher.select(domain),
            snapshot.policy.fallback_servers.as_slice(),
        ),
        DnsQueryRole::Direct => match snapshot.policy.direct_server.as_deref() {
            Some(server) => (server, snapshot.policy.direct_fallback_servers.as_slice()),
            None => (
                snapshot.dispatcher.select(domain),
                snapshot.policy.fallback_servers.as_slice(),
            ),
        },
        DnsQueryRole::Node => match snapshot.policy.node_server.as_deref() {
            Some(server) => (server, snapshot.policy.node_fallback_servers.as_slice()),
            None => (
                snapshot.dispatcher.select(domain),
                snapshot.policy.fallback_servers.as_slice(),
            ),
        },
    };
    let mut tags = Vec::with_capacity(1 + fallbacks.len());
    tags.push(selected);
    for fallback in fallbacks {
        if !tags.contains(&fallback.as_str()) {
            tags.push(fallback);
        }
    }

    let mut last_error = None;
    for tag in tags {
        let server = snapshot.servers.get(tag).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("DNS policy selected undefined backend `{tag}`"),
            )
        })?;
        let timeout_ms = snapshot.policy.timeout_ms_for(tag);
        let deadline = std::time::Duration::from_millis(timeout_ms);
        let attempt = match tokio::time::timeout(
            deadline,
            server.backend.exchange(
                query,
                server.detour.as_deref(),
                snapshot.outbound_connector.as_deref(),
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("DNS backend `{tag}` timed out after {}ms", timeout_ms),
            )),
        };
        match attempt.and_then(|response| {
            let parsed = message::parse_response(query, &response)?;
            if let Some(address) = parsed.addresses.iter().find(|address| {
                let address = ip_address_to_std(**address);
                snapshot
                    .policy
                    .reject_address_cidrs
                    .iter()
                    .any(|cidr| cidr.contains(&address))
            }) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "DNS backend `{tag}` returned rejected address {}",
                        format_ip_address(address)
                    ),
                ));
            }
            if matches!(
                parsed.response_code,
                message::RCODE_NOERROR | message::RCODE_NXDOMAIN
            ) {
                Ok((response, parsed))
            } else {
                Err(io::Error::other(format!(
                    "DNS backend `{tag}` returned retryable response code {}",
                    parsed.response_code
                )))
            }
        }) {
            Ok(response) => {
                record_query_attempt(snapshot, domain, role, tag, server, None);
                return Ok(response);
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    %domain,
                    role = role.as_str(),
                    backend = %tag,
                    "DNS backend attempt failed"
                );
                record_query_attempt(snapshot, domain, role, tag, server, Some(error.to_string()));
                last_error = Some(error);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "DNS policy has no backend")
    }))
}

fn record_query_attempt(
    snapshot: &ResolveSnapshot,
    domain: &str,
    role: DnsQueryRole,
    server_tag: &str,
    server: &ResolverServer,
    failure_reason: Option<String>,
) {
    const QUERY_ATTEMPT_CAPACITY: usize = 256;
    let mut attempts = snapshot
        .query_attempts
        .lock()
        .expect("DNS query attempt lock poisoned");
    if attempts.len() >= QUERY_ATTEMPT_CAPACITY {
        attempts.pop_front();
    }
    attempts.push_back(DnsQueryAttempt {
        domain: domain.to_owned(),
        role,
        server_tag: server_tag.to_owned(),
        transport: server.backend.transport_name(),
        server_endpoints: server.backend.endpoint_labels(),
        outbound: server.detour.as_deref().unwrap_or("direct").to_owned(),
        success: failure_reason.is_none(),
        failure_reason,
    });
}

fn compile_standalone_dispatch(
    config: Option<&DnsConfig>,
) -> io::Result<Option<zero_router::DomainDispatcher<String>>> {
    config
        .map(|config| {
            config
                .compile_dispatch(&[], None)
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

fn ip_address_to_std(ip: IpAddress) -> std::net::IpAddr {
    match ip {
        IpAddress::V4(octets) => std::net::IpAddr::V4(std::net::Ipv4Addr::from(octets)),
        IpAddress::V6(octets) => std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)),
    }
}
