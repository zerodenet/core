use super::*;
impl FakeIpAllocator {
    pub async fn alloc_ipv4(&self, domain: &str) -> io::Result<Option<IpAddress>> {
        self.alloc(domain, AddressFamily::Ipv4).await
    }

    pub async fn alloc_ipv6(&self, domain: &str) -> io::Result<Option<IpAddress>> {
        if self.ipv6_network.is_none() {
            return Ok(None);
        }
        self.alloc(domain, AddressFamily::Ipv6).await
    }

    async fn alloc(&self, domain: &str, family: AddressFamily) -> io::Result<Option<IpAddress>> {
        let domain = normalize_domain(domain)?;
        if crate::localhost::is_localhost(&domain) {
            return Ok(None);
        }
        let mut state = self.inner.lock().await;
        if state
            .persistence
            .as_ref()
            .is_some_and(|persistence| persistence.paused())
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Fake-IP journal query admission is paused",
            ));
        }
        let now = Instant::now();
        let now_unix_ms = if state.persistence.is_some() {
            unix_time_ms()?
        } else {
            0
        };
        expire_mappings(&mut state, now, now_unix_ms, self.ttl, &self.stats)?;
        prune_retired(&mut state, now);
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let expires_at_unix_ms = now_unix_ms.saturating_add(duration_millis(self.ttl));

        if let Some(address) = state
            .forward
            .get(&domain)
            .and_then(|mapping| mapping.address(family))
        {
            let addresses = state
                .forward
                .get(&domain)
                .map(|mapping| mapping.addresses().collect::<Vec<_>>())
                .unwrap_or_default();
            persist_addresses(&mut state, &domain, &addresses, expires_at_unix_ms)?;
            if let Some(mapping) = state.forward.get_mut(&domain) {
                mapping.expires_at = now + self.ttl;
                mapping.expires_at_unix_ms = expires_at_unix_ms;
                mapping.last_used = clock;
            }
            compact_if_needed(&mut state);
            return Ok(Some(to_trait_address(address)));
        }

        let Some(address) = allocate_free(
            &mut state,
            family,
            &self.ipv4_network,
            self.ipv6_network.as_ref(),
        ) else {
            self.stats.exhaustions.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };

        let victim =
            if !state.forward.contains_key(&domain) && state.forward.len() >= self.max_entries {
                state
                    .forward
                    .iter()
                    .min_by_key(|(_, mapping)| mapping.last_used)
                    .map(|(domain, _)| domain.clone())
            } else {
                None
            };
        let mut persisted_addresses = state
            .forward
            .get(&domain)
            .map(|mapping| mapping.addresses().collect::<Vec<_>>())
            .unwrap_or_default();
        persisted_addresses.push(address);
        let persisted = checkpoint::persist_allocation(
            &state,
            &domain,
            &persisted_addresses,
            expires_at_unix_ms,
            victim.as_deref(),
            now_unix_ms.saturating_add(duration_millis(self.ttl)),
        )?;
        if let Some(victim) = &victim {
            retire_domain_in_memory(
                &mut state,
                victim,
                RetiredAddress {
                    reusable_at: now + self.ttl,
                    reusable_after_unix_ms: now_unix_ms.saturating_add(duration_millis(self.ttl)),
                },
            );
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }

        if !persisted {
            persist_addresses(
                &mut state,
                &domain,
                &persisted_addresses,
                expires_at_unix_ms,
            )?;
        }

        state.reverse.insert(address, domain.clone());
        match state.forward.entry(domain) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let mapping = entry.get_mut();
                mapping.set_address(address);
                mapping.expires_at = now + self.ttl;
                mapping.expires_at_unix_ms = expires_at_unix_ms;
                mapping.last_used = clock;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let mut mapping = Mapping {
                    ipv4: None,
                    ipv6: None,
                    expires_at: now + self.ttl,
                    expires_at_unix_ms,
                    last_used: clock,
                };
                mapping.set_address(address);
                entry.insert(mapping);
            }
        }
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        compact_if_needed(&mut state);
        Ok(Some(to_trait_address(address)))
    }
}
