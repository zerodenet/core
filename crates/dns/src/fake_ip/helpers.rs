use super::*;
impl DomainExclusion {
    pub(super) fn as_pattern(&self) -> String {
        match self {
            Self::Exact(domain) => domain.clone(),
            Self::Suffix(domain) => format!("*.{domain}"),
        }
    }
}

pub(super) fn parse_exclusion(pattern: &str) -> Result<DomainExclusion, String> {
    let pattern = pattern.trim();
    if let Some(suffix) = pattern.strip_prefix("*.") {
        return normalize_domain(suffix)
            .map(DomainExclusion::Suffix)
            .map_err(|error| error.to_string());
    }
    normalize_domain(pattern)
        .map(DomainExclusion::Exact)
        .map_err(|error| error.to_string())
}

pub(super) fn usable_ipv4(network: &ipnet::Ipv4Net) -> usize {
    ((1_u128 << (32 - network.prefix_len())) - 2).min(usize::MAX as u128) as usize
}

pub(super) fn usable_ipv6(network: &ipnet::Ipv6Net) -> usize {
    let host_bits = 128 - network.prefix_len();
    if u32::from(host_bits) >= usize::BITS {
        usize::MAX
    } else {
        1_usize << host_bits
    }
}

pub(super) fn allocate_free(
    state: &mut AllocatorState,
    family: AddressFamily,
    ipv4_network: &ipnet::Ipv4Net,
    ipv6_network: Option<&ipnet::Ipv6Net>,
) -> Option<IpAddr> {
    match family {
        AddressFamily::Ipv4 => allocate_free_ipv4(state, ipv4_network).map(IpAddr::V4),
        AddressFamily::Ipv6 => allocate_free_ipv6(state, ipv6_network?).map(IpAddr::V6),
    }
}

pub(super) fn allocate_free_ipv4(
    state: &mut AllocatorState,
    network: &ipnet::Ipv4Net,
) -> Option<Ipv4Addr> {
    let base = u32::from(network.network());
    let last = u32::from(network.broadcast()) - 1;
    let start = state.next_ipv4;
    let mut candidate = start;
    loop {
        let address = Ipv4Addr::from(candidate);
        if !state.reverse.contains_key(&IpAddr::V4(address))
            && !state.retired.contains_key(&IpAddr::V4(address))
        {
            state.next_ipv4 = if candidate >= last {
                base + 1
            } else {
                candidate + 1
            };
            return Some(address);
        }
        candidate = if candidate >= last {
            base + 1
        } else {
            candidate + 1
        };
        if candidate == start {
            return None;
        }
    }
}

pub(super) fn allocate_free_ipv6(
    state: &mut AllocatorState,
    network: &ipnet::Ipv6Net,
) -> Option<Ipv6Addr> {
    let base = u128::from(network.network());
    let last = u128::from(network.broadcast());
    let start = state.next_ipv6?;
    let mut candidate = start;
    loop {
        let address = Ipv6Addr::from(candidate);
        if !state.reverse.contains_key(&IpAddr::V6(address))
            && !state.retired.contains_key(&IpAddr::V6(address))
        {
            state.next_ipv6 = Some(if candidate >= last {
                base
            } else {
                candidate + 1
            });
            return Some(address);
        }
        candidate = if candidate >= last {
            base
        } else {
            candidate + 1
        };
        if candidate == start {
            return None;
        }
    }
}

pub(super) fn persist_addresses(
    state: &mut AllocatorState,
    domain: &str,
    addresses: &[IpAddr],
    expires_at_unix_ms: u64,
) -> io::Result<()> {
    checkpoint::persist_upsert(state, domain, addresses, expires_at_unix_ms)
}

pub(super) fn compact_if_needed(state: &mut AllocatorState) {
    let live_records = state.forward.len().saturating_add(state.retired.len());
    let should_compact = state
        .persistence
        .as_ref()
        .is_some_and(|persistence| persistence.should_compact(live_records));
    if !should_compact {
        return;
    }
    let compacted = (|| {
        let checkpoint = checkpoint::Checkpoint::new(state, None, None)?;
        state
            .persistence
            .as_ref()
            .unwrap()
            .compact_checkpoint(&checkpoint)
    })();
    if let Err(error) = compacted {
        tracing::warn!(%error, "failed to compact Fake-IP persistence state");
    }
}

pub(super) fn restore_mappings(
    state: &mut AllocatorState,
    ipv4_network: &ipnet::Ipv4Net,
    ipv6_network: Option<&ipnet::Ipv6Net>,
    ttl: Duration,
    max_entries: usize,
    mappings: Vec<PersistedMapping>,
) -> io::Result<()> {
    let now_unix_ms = unix_time_ms()?;
    let now = Instant::now();
    for persisted in mappings {
        let domain = normalize_domain(&persisted.domain)?;
        let in_pool = match persisted.ip {
            IpAddr::V4(address) => {
                ipv4_network.contains(&address)
                    && address != ipv4_network.network()
                    && address != ipv4_network.broadcast()
            }
            IpAddr::V6(address) => ipv6_network.is_some_and(|network| network.contains(&address)),
        };
        if domain != persisted.domain || !in_pool || state.reverse.contains_key(&persisted.ip) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Fake-IP state contains an invalid or duplicate mapping",
            ));
        }
        let remaining_ms = persisted
            .expires_at_unix_ms
            .saturating_sub(now_unix_ms)
            .min(duration_millis(ttl));
        if remaining_ms == 0 {
            continue;
        }
        state.clock = state.clock.wrapping_add(1);
        let expires_at_unix_ms = now_unix_ms.saturating_add(remaining_ms);
        let clock = state.clock;
        let mapping = state
            .forward
            .entry(domain.clone())
            .or_insert_with(|| Mapping {
                ipv4: None,
                ipv6: None,
                expires_at: now + Duration::from_millis(remaining_ms),
                expires_at_unix_ms,
                last_used: clock,
            });
        if mapping.address(persisted.ip.into()).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Fake-IP state contains a duplicate domain family",
            ));
        }
        mapping.set_address(persisted.ip);
        mapping.expires_at = mapping
            .expires_at
            .min(now + Duration::from_millis(remaining_ms));
        mapping.expires_at_unix_ms = mapping.expires_at_unix_ms.min(expires_at_unix_ms);
        mapping.last_used = clock;
        state.reverse.insert(persisted.ip, domain);
    }
    if state.forward.len() > max_entries {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Fake-IP state exceeds configured domain capacity",
        ));
    }
    Ok(())
}

impl From<IpAddr> for AddressFamily {
    fn from(address: IpAddr) -> Self {
        match address {
            IpAddr::V4(_) => Self::Ipv4,
            IpAddr::V6(_) => Self::Ipv6,
        }
    }
}

pub(super) fn to_trait_address(address: IpAddr) -> IpAddress {
    match address {
        IpAddr::V4(address) => IpAddress::V4(address.octets()),
        IpAddr::V6(address) => IpAddress::V6(address.octets()),
    }
}

pub(super) fn to_std_address(address: IpAddress) -> IpAddr {
    match address {
        IpAddress::V4(octets) => IpAddr::V4(Ipv4Addr::from(octets)),
        IpAddress::V6(octets) => IpAddr::V6(Ipv6Addr::from(octets)),
    }
}

pub(super) fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
