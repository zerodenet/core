//! Exact prepared-token ownership across synchronous journal IO and callbacks.
use super::*;
pub(super) struct Commit<'a> {
    dns: &'a DnsSystem,
    token: Arc<PreparedDnsReload>,
}
impl<'a> Commit<'a> {
    pub(super) fn begin(dns: &'a DnsSystem, token: &DnsReloadResources) -> io::Result<Self> {
        let prepared = dns
            .prepared_reload
            .lock()
            .expect("DNS prepared reload poisoned");
        if !prepared
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(actual, &token.0))
            || dns.config_generation.load(Ordering::Acquire) != token.0.generation
        {
            return Err(stale());
        }
        if token.0.committing.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "same DNS commit operation remains live",
            ));
        }
        Ok(Self {
            dns,
            token: token.0.clone(),
        })
    }
    pub(super) fn publish(self, new_inner: Option<DnsSystemInner>) -> io::Result<()> {
        let mut prepared = self
            .dns
            .prepared_reload
            .lock()
            .expect("DNS prepared reload poisoned");
        if !prepared
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(actual, &self.token))
            || self.token.discard_requested.load(Ordering::Acquire)
            || self.dns.config_generation.load(Ordering::Acquire) != self.token.generation
        {
            drop(prepared);
            drop(new_inner);
            return Err(stale());
        }
        let previous = new_inner.map(|new_inner| {
            let mut inner = self.dns.inner.write().expect("DNS system poisoned");
            let previous = std::mem::replace(&mut *inner, new_inner);
            self.dns.config_generation.fetch_add(1, Ordering::AcqRel);
            previous
        });
        let retired = prepared.take();
        drop(prepared);
        // Original pools, allocator/lease, query pauses and arbitrary waker Drop
        // all run after the inner and candidate metadata locks are released.
        drop(previous);
        drop(retired);
        Ok(())
    }
}
impl Drop for Commit<'_> {
    fn drop(&mut self) {
        let mut prepared = self
            .dns
            .prepared_reload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.token.committing.store(false, Ordering::Release);
        let retired = if self.token.discard_requested.load(Ordering::Acquire)
            && prepared
                .as_ref()
                .is_some_and(|actual| Arc::ptr_eq(actual, &self.token))
        {
            prepared.take()
        } else {
            None
        };
        drop(prepared);
        drop(retired);
    }
}
fn stale() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "DNS resource candidate is no longer current",
    )
}

impl DnsSystem {
    /// Publish this exact candidate after resource retirement. Persistent Fake-IP
    /// state is opened here only, after the host accepts the full transaction.
    pub fn commit_reload_resources(&self, token: &DnsReloadResources) -> io::Result<()> {
        let commit = commit::Commit::begin(self, token)?;
        if !token.0.retired.load(Ordering::Acquire)
            && token
                .0
                .retiring
                .iter()
                .all(|backend| matches!(backend.as_ref(), ResolverBackend::System(_)))
        {
            let mut closed: Vec<&TokioSystemResolver> = Vec::new();
            for backend in &token.0.retiring {
                #[cfg(any(feature = "udp", feature = "doh", feature = "dot", feature = "doq"))]
                let ResolverBackend::System(resolver) = backend.as_ref() else {
                    unreachable!("retiring set contains only System backends")
                };
                #[cfg(not(any(
                    feature = "udp",
                    feature = "doh",
                    feature = "dot",
                    feature = "doq"
                )))]
                let ResolverBackend::System(resolver) = backend.as_ref();
                if !resolver.close_idle() {
                    for resolver in closed {
                        resolver.reopen_idle();
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "system DNS original lookup is still running",
                    ));
                }
                closed.push(resolver);
            }
            token.0.retired.store(true, Ordering::Release);
        }
        if !token.0.retired.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "affected DNS resources require async retirement before commit",
            ));
        }
        if token
            .0
            .servers
            .values()
            .any(|server| !server.backend.resource_admission_open())
        {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "prepared DNS backend was retired before commit",
            ));
        }
        if token.0.unchanged {
            // Preserve the exact live cache/allocator/backend objects and the
            // actual DNS fact. Equal JSON alone is insufficient: compiled
            // dispatch and each admitted original backend must also match.
            let inner = self.inner.read().expect("DNS system lock poisoned");
            if !inner.matches_reload(
                token.0.config.as_ref(),
                token.0.dispatch.as_ref(),
                &token.0.servers,
            ) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "unchanged DNS originals no longer match",
                ));
            }
            drop(inner);
            if let Some(allocator) = self.snapshot_fake_ip() {
                // A native journal can be replaced by a cap-only transaction
                // even when the DNS/cache/config objects are exactly retained.
                allocator.activate_persistence()?;
            }
            return commit.publish(None);
        }
        let state_lease = if token
            .0
            .config
            .as_ref()
            .and_then(DnsConfig::fake_ip)
            .is_some()
        {
            self.fake_ip_state_lease
                .lock()
                .expect("Fake-IP state lease lock poisoned")
                .clone()
        } else {
            None
        };
        let new_inner = Self::build_inner(
            token.0.config.as_ref(),
            token.0.dispatch.clone(),
            self.snapshot_fake_ip(),
            self.snapshot_reverse_mapping(),
            state_lease,
            token.0.servers.clone(),
        )?;
        if let DnsSystemInner::System(backend) = &new_inner {
            backend.register_owners(&self.transport_owners)?;
        }
        commit.publish(Some(new_inner))
    }
}
