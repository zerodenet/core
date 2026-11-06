//! Cold exact candidate preparation and confirmed persistent-resource retirement.
//! No new backend I/O starts here. Host supplies actual changed detour dependencies.
use super::*;
use std::{collections::BTreeSet, sync::atomic::AtomicBool};

pub(super) struct PreparedDnsReload {
    config: Option<DnsConfig>,
    dispatch: Option<zero_router::DomainDispatcher<String>>,
    servers: BTreeMap<String, ResolverServer>,
    retiring: Vec<Arc<ResolverBackend>>,
    retired: AtomicBool,
    generation: u64,
    unchanged: bool,
}
/// A token for one actual prepared DNS candidate. It contains no synthetic
/// resource identity and is valid only for the DNS system that prepared it.
#[derive(Clone)]
pub struct DnsReloadResources(Arc<PreparedDnsReload>);
impl fmt::Debug for DnsReloadResources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DnsReloadResources").finish_non_exhaustive()
    }
}

pub(super) fn build_servers(
    config: &DnsConfig,
    previous: &BTreeMap<String, ResolverServer>,
    affected_detours: &BTreeSet<String>,
    egress: &zero_platform_tokio::EgressInterfaceControl,
    owners: &Arc<transport_owners::TransportOwners>,
    owned_root: Option<&OwnedDnsRoot>,
) -> io::Result<BTreeMap<String, ResolverServer>> {
    config
        .servers
        .iter()
        .map(|(tag, config)| {
            let reusable = previous.get(tag).filter(|server| {
                server.config == *config
                    && server.backend.resource_admission_open()
                    && !config
                        .detour()
                        .is_some_and(|target| affected_detours.contains(target))
            });
            let server = match reusable {
                Some(server) => server.clone(),
                None => {
                    let backend = Arc::new(match owned_root {
                        Some(root) => root.build_backend(config, egress.clone())?,
                        None => ResolverBackend::build(config, egress.clone())?,
                    });
                    backend.register_owners(owners)?;
                    ResolverServer {
                        config: config.clone(),
                        backend,
                        detour: config.detour().map(ToOwned::to_owned),
                    }
                }
            };
            Ok((tag.clone(), server))
        })
        .collect()
}

impl DnsSystemInner {
    fn matches_reload(
        &self,
        config: Option<&DnsConfig>,
        dispatch: Option<&zero_router::DomainDispatcher<String>>,
        candidate_servers: &BTreeMap<String, ResolverServer>,
    ) -> bool {
        match self {
            Self::System(_) => {
                config.is_none() && dispatch.is_none() && candidate_servers.is_empty()
            }
            Self::Configured {
                source_config,
                servers,
                dispatcher,
                ..
            } => {
                config == Some(source_config.as_ref())
                    && dispatch.is_some_and(|candidate| dispatcher.matches(candidate))
                    && servers.len() == candidate_servers.len()
                    && servers.iter().all(|(tag, original)| {
                        candidate_servers.get(tag).is_some_and(|candidate| {
                            Arc::ptr_eq(&original.backend, &candidate.backend)
                                && original.backend.resource_admission_open()
                        })
                    })
            }
        }
    }
}

impl DnsSystem {
    /// Synchronous convenience for changes without persistent backend replacement.
    /// Replacing DoH/DoQ requires the explicit async retirement boundary below.
    pub fn reload(&self, config: Option<&DnsConfig>) -> io::Result<()> {
        self.reload_with_dispatch(config, compile_standalone_dispatch(config)?)
    }
    pub fn reload_with_dispatch(
        &self,
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
    ) -> io::Result<()> {
        // Explicit public reload invalidates query contexts and caches even
        // when its native backends are reusable. Host transactions use the
        // prepared-resource API to retain an unchanged DNS fact.
        self.prepare_reload_resources(config, dispatch, &BTreeSet::new(), true, None)?;
        let result = self.commit_prepared_reload();
        if result.is_err() {
            self.discard_prepared_reload();
        }
        result
    }
    /// Validate and stage a candidate without closing current resources or
    /// replacing the committed Fake-IP journal. Existing backends are reused.
    pub fn prepare_reload_with_dispatch(
        &self,
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
    ) -> io::Result<()> {
        self.prepare_reload_resources_with_dispatch(config, dispatch, &BTreeSet::new())
            .map(|_| ())
    }
    /// `affected_detours` must come from the host's comparison of the actual
    /// selected prefix owners. A target name alone is not a resource identity.
    pub fn prepare_reload_resources_with_dispatch(
        &self,
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
        affected_detours: &BTreeSet<String>,
    ) -> io::Result<DnsReloadResources> {
        self.prepare_reload_resources(config, dispatch, affected_detours, false, None)
    }
    /// The host supplies its actual cold candidate's exact instance/Scope
    /// preservation decision. URI/config equality cannot preserve a physical
    /// pool whose original canonical provider is being replaced.
    pub fn prepare_reload_resources_with_host_scopes(
        &self,
        config: Option<&DnsConfig>,
        dispatch: Option<zero_router::DomainDispatcher<String>>,
        affected_detours: &BTreeSet<String>,
        preserves: impl Fn(
            zero_core::component::ComponentInstanceId,
            &zero_platform_tokio::contract::scope::TaskScopeIdentity,
        ) -> bool,
    ) -> io::Result<DnsReloadResources> {
        self.prepare_reload_resources(config, dispatch, affected_detours, false, Some(&preserves))
    }
    fn prepare_reload_resources(
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
        });
        *prepared = Some(candidate.clone());
        Ok(DnsReloadResources(candidate))
    }
    pub fn prepared_reload_resources(&self) -> io::Result<DnsReloadResources> {
        self.prepared_reload
            .lock()
            .expect("DNS prepared reload lock poisoned")
            .clone()
            .map(DnsReloadResources)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no DNS reload is prepared"))
    }
    fn ensure_candidate(&self, token: &DnsReloadResources) -> io::Result<()> {
        let prepared = self
            .prepared_reload
            .lock()
            .expect("DNS prepared reload lock poisoned");
        if !prepared
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(candidate, &token.0))
            || self.config_generation.load(Ordering::Acquire) != token.0.generation
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "DNS resource candidate is no longer current",
            ));
        }
        Ok(())
    }
    /// Stop admission to all changed/removed persistent backends, then confirm
    /// their actual users and drivers. Deadline/caller cancellation retains the
    /// same owners for retry; this method neither publishes nor starts a backend.
    pub async fn retire_reload_resources(
        &self,
        token: &DnsReloadResources,
        deadline: tokio::time::Instant,
    ) -> io::Result<()> {
        self.ensure_candidate(token)?;
        for backend in &token.0.retiring {
            backend.retire_resource_admission();
        }
        let mut failure = None;
        for backend in &token.0.retiring {
            if let Err(source) = backend.shutdown_resources(deadline).await {
                failure.get_or_insert(source);
            }
        }
        if let Some(source) = failure {
            return Err(source);
        }
        self.ensure_candidate(token)?;
        token.0.retired.store(true, Ordering::Release);
        Ok(())
    }
    /// Publish this exact candidate after resource retirement. Persistent Fake-IP
    /// state is opened here only, after the host accepts the full transaction.
    pub fn commit_reload_resources(&self, token: &DnsReloadResources) -> io::Result<()> {
        let mut prepared = self
            .prepared_reload
            .lock()
            .expect("DNS prepared reload lock poisoned");
        if !prepared
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(candidate, &token.0))
            || self.config_generation.load(Ordering::Acquire) != token.0.generation
        {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "DNS resource candidate is no longer current",
            ));
        }
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
            prepared.take();
            return Ok(());
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
        {
            let mut inner = self.inner.write().expect("DNS system lock poisoned");
            *inner = new_inner;
            self.config_generation.fetch_add(1, Ordering::AcqRel);
        }
        prepared.take();
        Ok(())
    }
    pub fn commit_prepared_reload(&self) -> io::Result<()> {
        self.commit_reload_resources(&self.prepared_reload_resources()?)
    }
    /// Discarding a retired candidate does not revive closed pools. Recovery
    /// prepares the previous config again and builds its retired backends anew.
    pub fn discard_prepared_reload(&self) {
        self.prepared_reload
            .lock()
            .expect("DNS prepared reload lock poisoned")
            .take();
    }
}

#[cfg(test)]
mod tests;
