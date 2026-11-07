//! Freeze exact DNS backends and their canonical host Scope preservation.
use super::*;
impl DnsSystem {
    pub(super) fn prepare_reload_resources(
        &self,
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
        affected_detours: &BTreeSet<String>,
        refresh_queries: bool,
        preserves: Option<
            &dyn Fn(
                zero_core::component::ComponentInstanceId,
                &zero_platform_tokio::contract::scope::TaskScopeIdentity,
            ) -> bool,
        >,
    ) -> io::Result<DnsReloadResources> {
        if self.owned_root.is_some() && config.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "owned DNS UDP reload requires its explicit configured root",
            ));
        }
        let mut prepared = self
            .prepared_reload
            .lock()
            .expect("DNS prepared reload lock poisoned");
        if prepared.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "another DNS reload is already prepared",
            ));
        }
        let (previous, previous_system, generation, native_profile_changed) = {
            let inner = self.inner.read().expect("DNS system lock poisoned");
            let servers = match &*inner {
                DnsSystemInner::Configured { servers, .. } => servers.clone(),
                _ => BTreeMap::new(),
            };
            let system = match &*inner {
                DnsSystemInner::System(backend) => Some(backend.clone()),
                _ => None,
            };
            (
                servers,
                system,
                self.config_generation.load(Ordering::Acquire),
                (self.native_source.is_some() || self.node_queries.is_some())
                    && match &*inner {
                        DnsSystemInner::Configured {
                            source_config,
                            dispatcher,
                            ..
                        } => {
                            config != Some(source_config.as_ref())
                                || !dispatch
                                    .as_ref()
                                    .is_some_and(|next| dispatcher.matches(next))
                        }
                        DnsSystemInner::System(_) => config.is_some(),
                    },
            )
        };
        let original_servers = previous.clone();
        #[cfg(feature = "doh")]
        let previous = {
            let mut reusable = previous;
            reusable.retain(|_, server| match server.backend.as_ref() {
                ResolverBackend::Doh(resolver) => {
                    !(native_profile_changed && resolver.owner.host_bound())
                        && preserves
                            .is_none_or(|preserves| resolver.owner.preserves_host_scope(preserves))
                }
                _ => true,
            });
            reusable
        };
        #[cfg(not(feature = "doh"))]
        let _ = (native_profile_changed, preserves);
        let servers = config
            .map(|config| {
                build_servers(
                    config,
                    &previous,
                    affected_detours,
                    &self.egress_interface,
                    &self.transport_owners,
                    self.owned_root.as_deref(),
                )
            })
            .transpose()?
            .unwrap_or_default();
        let unchanged = !refresh_queries
            && self
                .inner
                .read()
                .expect("DNS system lock poisoned")
                .matches_reload(config, dispatch.as_ref(), &servers);
        if !unchanged && config.and_then(DnsConfig::fake_ip).is_some() {
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
        }
        // Validate allocator fields in memory; a rejected candidate must not
        // replace a durable Fake-IP journal. Keep these exact backend objects.
        if !unchanged {
            Self::build_inner(
                config,
                dispatch.clone(),
                self.snapshot_fake_ip(),
                self.snapshot_reverse_mapping(),
                None,
                servers.clone(),
            )?;
        }
        let mut retiring = original_servers
            .values()
            .filter(|old| {
                old.backend.has_persistent_resources()
                    && !servers
                        .values()
                        .any(|new| Arc::ptr_eq(&old.backend, &new.backend))
            })
            .map(|old| old.backend.clone())
            .collect::<Vec<_>>();
        if !unchanged {
            retiring.extend(previous_system);
        }
        let candidate = Arc::new(PreparedDnsReload {
            config: config.cloned(),
            dispatch,
            servers,
            retired: AtomicBool::new(retiring.is_empty()),
            retiring,
            generation,
            unchanged,
            native_profile_changed,
            donor_pause: Default::default(),
        });
        *prepared = Some(candidate.clone());
        Ok(DnsReloadResources(candidate))
    }
}
