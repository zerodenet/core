//! Fake-IP allocator: maps domains to synthetic IPs for transparent proxying.
//!
//! When a client queries `google.com`, we return `198.18.0.5` instead
//! of the real IP. When the client later connects to `198.18.0.5:443`,
//! we look up `google.com` from the reverse map and route based on the
//! real domain name.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use zero_config::FakeIpConfigRef;
use zero_traits::IpAddress;

/// Allocates fake IPs from a configurable CIDR pool.
pub struct FakeIpAllocator {
    inner: Mutex<AllocatorInner>,
    network: ipnet::Ipv4Net,
    ttl: Duration,
    exclude_domains: Vec<String>,
}

struct AllocatorInner {
    /// The next IP to try allocating (linear scan from network base).
    next_ip: u32,
    /// Network base as u32.
    base: u32,
    /// Subnet mask.
    mask: u32,
    /// Domain to assigned fake IP (`IpAddress::V4`).
    forward: HashMap<String, IpAddress>,
    /// Fake-IP bytes to domain.
    reverse: HashMap<[u8; 4], (String, Instant)>,
}

impl FakeIpAllocator {
    /// Parse the CIDR and build the allocator.
    pub fn new(config: FakeIpConfigRef<'_>) -> Result<Self, String> {
        let net: ipnet::IpNet = config
            .cidr
            .parse()
            .map_err(|e| format!("invalid cidr: {e}"))?;
        let network = match net {
            ipnet::IpNet::V4(network) => network,
            ipnet::IpNet::V6(_) => return Err("fake ip only supports IPv4 CIDR".into()),
        };
        if network.prefix_len() > 30 {
            return Err("fake ip IPv4 CIDR must contain at least four addresses".into());
        }
        if config.ttl_seconds == 0 {
            return Err("fake ip TTL must be greater than zero".into());
        }
        let base = u32::from_be_bytes(network.network().octets());
        let mask = u32::from_be_bytes(network.netmask().octets());
        let next_ip = base
            .checked_add(1)
            .ok_or_else(|| "fake ip CIDR has no allocatable address".to_owned())?;

        Ok(Self {
            inner: Mutex::new(AllocatorInner {
                next_ip, // skip network address
                base,
                mask,
                forward: HashMap::new(),
                reverse: HashMap::new(),
            }),
            network,
            ttl: Duration::from_secs(config.ttl_seconds),
            exclude_domains: config.exclude_domains.to_vec(),
        })
    }

    pub fn contains(&self, address: std::net::IpAddr) -> bool {
        match address {
            std::net::IpAddr::V4(address) => self.network.contains(&address),
            std::net::IpAddr::V6(_) => false,
        }
    }

    /// Check if a domain should skip fake IP.
    pub fn is_excluded(&self, domain: &str) -> bool {
        let domain = domain.to_ascii_lowercase();
        self.exclude_domains.iter().any(|pattern| {
            if let Some(suffix) = pattern.strip_prefix('*') {
                domain.ends_with(suffix)
            } else {
                pattern.as_str() == domain
            }
        })
    }

    /// Allocate a fake IP for a domain, or return the existing one.
    /// Returns `None` if the pool is exhausted.
    pub async fn alloc(&self, domain: &str) -> Option<IpAddress> {
        let mut inner = self.inner.lock().await;

        // Copy the existing IP before mutably borrowing the reverse map.
        if let Some(ip) = inner.forward.get(domain) {
            let existing = *ip;
            // Refresh TTL.
            if let IpAddress::V4(octets) = existing {
                if let Some(entry) = inner.reverse.get_mut(&octets) {
                    entry.1 = Instant::now() + self.ttl;
                }
            }
            return Some(existing);
        }

        // Allocate new.
        let broadcast = inner.base | !inner.mask;
        let start = inner.next_ip;
        let mut ip = start;
        loop {
            let octets = u32::to_be_bytes(ip);
            // Don't use network address, broadcast, or already-assigned but expired IPs.
            if ip != inner.base && ip != broadcast {
                match inner.reverse.get(&octets) {
                    None => {
                        // Free address: use it.
                        inner
                            .forward
                            .insert(domain.to_owned(), IpAddress::V4(octets));
                        inner
                            .reverse
                            .insert(octets, (domain.to_owned(), Instant::now() + self.ttl));
                        inner.next_ip = if ip + 1 > broadcast - 1 {
                            inner.base + 1
                        } else {
                            ip + 1
                        };
                        return Some(IpAddress::V4(octets));
                    }
                    Some((_, expires)) if *expires <= Instant::now() => {
                        // Expired address: reclaim it.
                        let old_domain = inner.reverse.remove(&octets).unwrap().0;
                        inner.forward.remove(&old_domain);
                        inner
                            .forward
                            .insert(domain.to_owned(), IpAddress::V4(octets));
                        inner
                            .reverse
                            .insert(octets, (domain.to_owned(), Instant::now() + self.ttl));
                        inner.next_ip = if ip + 1 > broadcast - 1 {
                            inner.base + 1
                        } else {
                            ip + 1
                        };
                        return Some(IpAddress::V4(octets));
                    }
                    Some(_) => { /* in use, skip */ }
                }
            }
            if ip >= broadcast - 1 {
                ip = inner.base + 1;
            } else {
                ip += 1;
            }
            if ip == start {
                return None; // pool exhausted
            }
        }
    }

    /// Reverse lookup: fake IP to domain.
    pub async fn lookup(&self, ip: &IpAddress) -> Option<String> {
        let octets = match ip {
            IpAddress::V4(o) => *o,
            _ => return None,
        };
        let inner = self.inner.lock().await;
        inner.reverse.get(&octets).map(|(d, _)| d.clone())
    }

    /// Forward lookup (diagnostic): domain to assigned fake IP, without
    /// allocating a new one. Returns `None` if the domain has no mapping.
    pub async fn lookup_domain(&self, domain: &str) -> Option<IpAddress> {
        let inner = self.inner.lock().await;
        inner.forward.get(domain).copied()
    }

    /// Evict expired entries. Call periodically or on allocation.
    #[allow(dead_code)]
    pub async fn evict_expired(&self) {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let expired: Vec<[u8; 4]> = inner
            .reverse
            .iter()
            .filter(|(_, (_, expires))| *expires <= now)
            .map(|(octets, _)| *octets)
            .collect();
        for octets in expired {
            if let Some((domain, _)) = inner.reverse.remove(&octets) {
                inner.forward.remove(&domain);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> FakeIpConfigRef<'static> {
        FakeIpConfigRef {
            cidr: "198.18.0.0/24",
            ttl_seconds: 3600,
            exclude_domains: &[],
        }
    }

    #[tokio::test]
    async fn alloc_and_lookup() {
        let alloc = FakeIpAllocator::new(test_config()).unwrap();
        let ip = alloc.alloc("google.com").await.unwrap();
        assert_eq!(alloc.lookup(&ip).await.unwrap(), "google.com");
    }

    #[tokio::test]
    async fn same_domain_same_ip() {
        let alloc = FakeIpAllocator::new(test_config()).unwrap();
        let ip1 = alloc.alloc("google.com").await.unwrap();
        let ip2 = alloc.alloc("google.com").await.unwrap();
        assert_eq!(ip1, ip2);
    }

    #[tokio::test]
    async fn different_domains_different_ips() {
        let alloc = FakeIpAllocator::new(test_config()).unwrap();
        let ip1 = alloc.alloc("google.com").await.unwrap();
        let ip2 = alloc.alloc("github.com").await.unwrap();
        assert_ne!(ip1, ip2);
    }

    #[tokio::test]
    async fn smallest_valid_pool_exhausts_without_wrapping() {
        let alloc = FakeIpAllocator::new(FakeIpConfigRef {
            cidr: "255.255.255.252/30",
            ..test_config()
        })
        .unwrap();
        assert!(alloc.alloc("one.example").await.is_some());
        assert!(alloc.alloc("two.example").await.is_some());
        assert!(alloc.alloc("three.example").await.is_none());
    }

    #[tokio::test]
    async fn excluded_domain() {
        let excluded = ["*.local".into(), "example.com".into()];
        let alloc = FakeIpAllocator::new(FakeIpConfigRef {
            exclude_domains: &excluded,
            ..test_config()
        })
        .unwrap();
        assert!(alloc.is_excluded("app.local"));
        assert!(alloc.is_excluded("example.com"));
        assert!(!alloc.is_excluded("google.com"));
    }

    #[tokio::test]
    async fn lookup_domain_returns_existing_without_allocating() {
        let alloc = FakeIpAllocator::new(test_config()).unwrap();
        let allocated = alloc.alloc("google.com").await.unwrap();
        // Forward lookup of an allocated domain returns the same IP.
        assert_eq!(alloc.lookup_domain("google.com").await, Some(allocated));
        // Unknown domain yields None and must not allocate.
        assert_eq!(alloc.lookup_domain("never-seen.example").await, None);
    }
}
