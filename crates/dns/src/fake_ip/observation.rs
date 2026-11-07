use super::*;
impl FakeIpAllocator {
    pub async fn lookup(&self, ip: &IpAddress) -> Option<String> {
        let address = to_std_address(*ip);
        if !self.contains(address) {
            return None;
        }
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        let paused = state
            .persistence
            .as_ref()
            .is_some_and(|persistence| persistence.paused());
        if !paused {
            prune_retired(&mut state, now);
        }
        let Some(domain) = state.reverse.get(&address).cloned() else {
            self.stats.reverse_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if state
            .forward
            .get(&domain)
            .is_none_or(|mapping| mapping.expires_at <= now)
        {
            if paused {
                return None;
            }
            let now_unix_ms = unix_time_ms().unwrap_or_default();
            if let Err(error) = retire_domain(&mut state, &domain, now, now_unix_ms, self.ttl) {
                tracing::warn!(%error, %domain, "failed to persist expired Fake-IP retirement");
            }
            self.stats.expirations.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        if let Some(mapping) = state.forward.get_mut(&domain) {
            mapping.last_used = clock;
        }
        Some(domain)
    }

    pub async fn lookup_domain(&self, domain: &str) -> Option<IpAddress> {
        let domain = normalize_domain(domain).ok()?;
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        let paused = state
            .persistence
            .as_ref()
            .is_some_and(|persistence| persistence.paused());
        if !paused {
            prune_retired(&mut state, now);
        }
        if state
            .forward
            .get(&domain)
            .is_none_or(|mapping| mapping.expires_at <= now)
        {
            if !paused && state.forward.contains_key(&domain) {
                let now_unix_ms = unix_time_ms().unwrap_or_default();
                if let Err(error) = retire_domain(&mut state, &domain, now, now_unix_ms, self.ttl) {
                    tracing::warn!(%error, %domain, "failed to persist expired Fake-IP retirement");
                }
                self.stats.expirations.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let mapping = state.forward.get_mut(&domain)?;
        mapping.last_used = clock;
        mapping
            .ipv4
            .map(|address| IpAddress::V4(address.octets()))
            .or_else(|| mapping.ipv6.map(|address| IpAddress::V6(address.octets())))
    }

    pub async fn stats(&self) -> FakeIpStats {
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        let now_unix_ms = unix_time_ms().unwrap_or_default();
        let paused = state
            .persistence
            .as_ref()
            .is_some_and(|persistence| persistence.paused());
        if !paused {
            if let Err(error) = expire_mappings(&mut state, now, now_unix_ms, self.ttl, &self.stats)
            {
                tracing::warn!(%error, "failed to persist expired Fake-IP retirements");
            }
            prune_retired(&mut state, now);
        }
        FakeIpStats {
            allocations: self.stats.allocations.load(Ordering::Relaxed),
            expirations: self.stats.expirations.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            exhaustions: self.stats.exhaustions.load(Ordering::Relaxed),
            collisions: self.stats.collisions.load(Ordering::Relaxed),
            reverse_misses: self.stats.reverse_misses.load(Ordering::Relaxed),
            live_mappings: state
                .forward
                .values()
                .filter(|mapping| mapping.expires_at > now)
                .count(),
            retired_addresses: state
                .retired
                .values()
                .filter(|address| address.reusable_at > now)
                .count(),
            capacity: self.max_entries,
        }
    }
}
