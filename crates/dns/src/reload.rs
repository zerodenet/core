//! Cold exact candidate preparation and confirmed persistent-resource retirement.
//! No new backend I/O starts here. Host supplies actual changed detour dependencies.
use super::*;
use std::{collections::BTreeSet, sync::atomic::AtomicBool};
mod commit;
mod donor_pause;
mod prepare;

pub(super) struct PreparedDnsReload {
    config: Option<DnsConfig>,
    dispatch: Option<zero_router::DomainDispatcher<String>>,
    servers: BTreeMap<String, ResolverServer>,
    retiring: Vec<Arc<ResolverBackend>>,
    retired: AtomicBool,
    generation: u64,
    unchanged: bool,
    native_profile_changed: bool,
    donor_pause: std::sync::Mutex<Option<Arc<crate::DnsDonorPause>>>,
    fake_ip_journal_limit: std::sync::Mutex<Option<usize>>,
    committing: AtomicBool,
    discard_requested: AtomicBool,
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
        if token.0.committing.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "same DNS commit operation remains live",
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
    pub fn commit_prepared_reload(&self) -> io::Result<()> {
        self.commit_reload_resources(&self.prepared_reload_resources()?)
    }
    /// Discarding a retired candidate does not revive closed pools. Recovery
    /// prepares the previous config again and builds its retired backends anew.
    pub fn discard_prepared_reload(&self) {
        let mut prepared = self
            .prepared_reload
            .lock()
            .expect("DNS prepared reload poisoned");
        let retired = if prepared
            .as_ref()
            .is_some_and(|actual| actual.committing.load(Ordering::Acquire))
        {
            prepared
                .as_ref()
                .unwrap()
                .discard_requested
                .store(true, Ordering::Release);
            None
        } else {
            prepared.take()
        };
        drop(prepared);
        drop(retired);
    }
}

#[cfg(test)]
mod tests;
