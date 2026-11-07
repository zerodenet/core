use super::*;
impl FakeIpAllocator {
    #[cfg(test)]
    pub fn new(config: FakeIpConfigRef<'_>) -> Result<Self, String> {
        Self::new_with_state(config, None).map_err(|error| error.to_string())
    }

    pub fn new_with_state(
        config: FakeIpConfigRef<'_>,
        state_lease: Option<Arc<FakeIpStateLease>>,
    ) -> io::Result<Self> {
        let retained = state_lease.clone();
        Self::construct(config, state_lease).map_err(|work| match retained {
            Some(lease) => lease.retain_open_failure(work),
            None => work,
        })
    }
    fn construct(
        config: FakeIpConfigRef<'_>,
        state_lease: Option<Arc<FakeIpStateLease>>,
    ) -> io::Result<Self> {
        let ipv4_network = config.cidr.parse::<ipnet::Ipv4Net>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid IPv4 Fake-IP CIDR: {error}"),
            )
        })?;
        if ipv4_network.prefix_len() > 30 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP IPv4 CIDR must contain at least four addresses",
            ));
        }
        let ipv6_network = config
            .ipv6_cidr
            .map(|cidr| {
                cidr.parse::<ipnet::Ipv6Net>().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid IPv6 Fake-IP CIDR: {error}"),
                    )
                })
            })
            .transpose()?;
        if ipv6_network.is_some_and(|network| network.prefix_len() > 126) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP IPv6 CIDR must contain at least four addresses",
            ));
        }
        if config.ttl_seconds == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP TTL must be greater than zero",
            ));
        }

        let usable = ipv6_network.map_or_else(
            || usable_ipv4(&ipv4_network),
            |network| usable_ipv4(&ipv4_network).min(usable_ipv6(&network)),
        );
        let max_entries = config
            .max_entries
            .unwrap_or(DEFAULT_MAX_ENTRIES)
            .min(usable);
        if max_entries == 0
            || config
                .max_entries
                .is_some_and(|configured| configured > usable)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Fake-IP max_entries must be between 1 and {usable}"),
            ));
        }
        let exclusions = config
            .exclude_domains
            .iter()
            .map(|pattern| parse_exclusion(pattern))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let metadata = PersistenceMetadata {
            cidr: ipv4_network.to_string(),
            ipv6_cidr: ipv6_network.map(|network| network.to_string()),
            ttl_seconds: config.ttl_seconds,
            max_entries,
            exclusions: exclusions.iter().map(DomainExclusion::as_pattern).collect(),
        };
        let (persistence, recovered) = match state_lease {
            Some(lease) => {
                let (persistence, mappings) = FakeIpPersistence::open(lease, metadata)?;
                (Some(persistence), mappings)
            }
            None => (None, Default::default()),
        };
        let mut state = AllocatorState {
            next_ipv4: u32::from(ipv4_network.network()) + 1,
            next_ipv6: ipv6_network.map(|network| u128::from(network.network())),
            clock: 0,
            forward: HashMap::new(),
            reverse: HashMap::new(),
            retired: HashMap::new(),
            persistence,
        };
        let restored = restore_mappings(
            &mut state,
            &ipv4_network,
            ipv6_network.as_ref(),
            Duration::from_secs(config.ttl_seconds),
            max_entries,
            recovered.mappings,
        )
        .and_then(|()| {
            restore_retired_addresses(
                &mut state,
                &ipv4_network,
                ipv6_network.as_ref(),
                Duration::from_secs(config.ttl_seconds),
                recovered.retired,
            )
        });
        if let Err(error) = restored {
            tracing::warn!(%error, "discarding invalid Fake-IP persistence state");
            state.forward.clear();
            state.reverse.clear();
            state.retired.clear();
            if let Some(persistence) = state.persistence.as_mut() {
                persistence.compact(&[], &[])?;
            }
        }
        Ok(Self {
            inner: Mutex::new(state),
            ipv4_network,
            ipv6_network,
            ttl: Duration::from_secs(config.ttl_seconds),
            max_entries,
            exclusions,
            stats: FakeIpCounters::default(),
        })
    }

    pub fn compatible_with(&self, config: FakeIpConfigRef<'_>) -> bool {
        let Ok(ipv4_network) = config.cidr.parse::<ipnet::Ipv4Net>() else {
            return false;
        };
        let Ok(ipv6_network) = config
            .ipv6_cidr
            .map(str::parse::<ipnet::Ipv6Net>)
            .transpose()
        else {
            return false;
        };
        let usable = ipv6_network.map_or_else(
            || usable_ipv4(&ipv4_network),
            |network| usable_ipv4(&ipv4_network).min(usable_ipv6(&network)),
        );
        let max_entries = config
            .max_entries
            .unwrap_or(DEFAULT_MAX_ENTRIES)
            .min(usable);
        let Ok(exclusions) = config
            .exclude_domains
            .iter()
            .map(|pattern| parse_exclusion(pattern))
            .collect::<Result<Vec<_>, _>>()
        else {
            return false;
        };
        self.ipv4_network == ipv4_network
            && self.ipv6_network == ipv6_network
            && self.ttl == Duration::from_secs(config.ttl_seconds)
            && self.max_entries == max_entries
            && self.exclusions == exclusions
    }

    pub fn contains(&self, address: IpAddr) -> bool {
        match address {
            IpAddr::V4(address) => self.ipv4_network.contains(&address),
            IpAddr::V6(address) => self
                .ipv6_network
                .as_ref()
                .is_some_and(|network| network.contains(&address)),
        }
    }

    pub fn ttl_seconds(&self) -> u32 {
        self.ttl.as_secs().min(u64::from(u32::MAX)) as u32
    }

    pub(crate) fn ipv6_enabled(&self) -> bool {
        self.ipv6_network.is_some()
    }

    pub fn record_collision(&self) {
        self.stats.collisions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn is_excluded(&self, domain: &str) -> bool {
        let Ok(domain) = normalize_domain(domain) else {
            return true;
        };
        if crate::localhost::is_localhost(&domain) {
            return true;
        }
        self.exclusions.iter().any(|pattern| match pattern {
            DomainExclusion::Exact(exact) => domain == *exact,
            DomainExclusion::Suffix(suffix) => {
                domain == *suffix || domain.ends_with(&format!(".{suffix}"))
            }
        })
    }
}
