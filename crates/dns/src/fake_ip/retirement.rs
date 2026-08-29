use std::io;
use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::persistence::{unix_time_ms, PersistedRetiredAddress};
use super::{duration_millis, AllocatorState, FakeIpCounters, RetiredAddress};

pub(super) fn expire_mappings(
    state: &mut AllocatorState,
    now: Instant,
    now_unix_ms: u64,
    retirement_ttl: Duration,
    stats: &FakeIpCounters,
) -> io::Result<()> {
    let expired = state
        .forward
        .iter()
        .filter(|(_, mapping)| mapping.expires_at <= now)
        .map(|(domain, _)| domain.clone())
        .collect::<Vec<_>>();
    let mut first_error = None;
    for domain in expired {
        if let Err(error) = retire_domain(state, &domain, now, now_unix_ms, retirement_ttl) {
            first_error.get_or_insert(error);
        }
        stats.expirations.fetch_add(1, Ordering::Relaxed);
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(super) fn retire_domain(
    state: &mut AllocatorState,
    domain: &str,
    now: Instant,
    now_unix_ms: u64,
    retirement_ttl: Duration,
) -> io::Result<usize> {
    let Some(addresses) = state
        .forward
        .get(domain)
        .map(|mapping| mapping.addresses().collect::<Vec<_>>())
    else {
        return Ok(0);
    };
    let retirement = RetiredAddress {
        reusable_at: now + retirement_ttl,
        reusable_after_unix_ms: now_unix_ms.saturating_add(duration_millis(retirement_ttl)),
    };
    let mut persistence_error = None;
    if let Some(persistence) = state.persistence.as_mut() {
        for address in &addresses {
            if let Err(error) = persistence.append_retire(&PersistedRetiredAddress {
                ip: *address,
                reusable_after_unix_ms: retirement.reusable_after_unix_ms,
            }) {
                persistence_error.get_or_insert(error);
                break;
            }
        }
    }

    retire_domain_in_memory(state, domain, retirement);
    match persistence_error {
        Some(error) => Err(error),
        None => Ok(addresses.len()),
    }
}

pub(super) fn retire_domain_in_memory(
    state: &mut AllocatorState,
    domain: &str,
    retirement: RetiredAddress,
) -> usize {
    let Some(mapping) = state.forward.remove(domain) else {
        return 0;
    };
    let addresses = mapping.addresses().collect::<Vec<_>>();
    for address in &addresses {
        state.reverse.remove(address);
        state.retired.insert(*address, retirement);
    }
    addresses.len()
}

pub(super) fn prune_retired(state: &mut AllocatorState, now: Instant) {
    state.retired.retain(|_, retired| retired.reusable_at > now);
}

pub(super) fn persisted_retired_addresses(state: &AllocatorState) -> Vec<PersistedRetiredAddress> {
    let mut retired = state
        .retired
        .iter()
        .map(|(ip, retired)| PersistedRetiredAddress {
            ip: *ip,
            reusable_after_unix_ms: retired.reusable_after_unix_ms,
        })
        .collect::<Vec<_>>();
    retired.sort_by_key(|entry| (entry.reusable_after_unix_ms, entry.ip));
    retired
}

pub(super) fn restore_retired_addresses(
    state: &mut AllocatorState,
    ipv4_network: &ipnet::Ipv4Net,
    ipv6_network: Option<&ipnet::Ipv6Net>,
    retirement_ttl: Duration,
    retired: Vec<PersistedRetiredAddress>,
) -> io::Result<()> {
    let now_unix_ms = unix_time_ms()?;
    let now = Instant::now();
    for retired in retired {
        let in_pool = match retired.ip {
            IpAddr::V4(address) => {
                ipv4_network.contains(&address)
                    && address != ipv4_network.network()
                    && address != ipv4_network.broadcast()
            }
            IpAddr::V6(address) => ipv6_network.is_some_and(|network| network.contains(&address)),
        };
        if !in_pool || state.reverse.contains_key(&retired.ip) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Fake-IP state contains an invalid or live retired address",
            ));
        }
        let remaining_ms = retired
            .reusable_after_unix_ms
            .saturating_sub(now_unix_ms)
            .min(duration_millis(retirement_ttl));
        if remaining_ms == 0 {
            continue;
        }
        state.retired.insert(
            retired.ip,
            RetiredAddress {
                reusable_at: now + Duration::from_millis(remaining_ms),
                reusable_after_unix_ms: now_unix_ms.saturating_add(remaining_ms),
            },
        );
    }
    Ok(())
}
