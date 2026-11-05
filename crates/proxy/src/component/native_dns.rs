//! Actual roleless DNS query owner in the same RuntimeHost as its consumers.
use super::{background::BackgroundNetwork, RegistrationError, ResourcePreparation};
use std::{
    hash::{BuildHasher, Hasher},
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};
use zero_core::component::ComponentId;
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceDeclaration, ResourceOwner,
        },
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
        scope::TaskRegistrar,
        CloseReason,
    },
    controlled_carrier::CarrierOwner,
};
mod provider;
use provider::Provider;
use zero_dns::native::NativeDnsProvider;
#[cfg(test)]
pub(crate) const LIMIT: usize = 16;
pub(crate) type Budget = zero_dns::native::NativeDnsBudget;
use zero_dns::native::NativeDnsBudgetLimits;
fn candidate_budget(
    network: &BackgroundNetwork,
    limits: NativeDnsBudgetLimits,
) -> Result<Arc<Budget>, RegistrationError> {
    let node = network
        .native_dns
        .current_budget()
        .filter(|budget| budget.limits() == limits);
    let query = network
        .query_dns
        .current_budget()
        .filter(|budget| budget.limits() == limits);
    if node
        .as_ref()
        .zip(query.as_ref())
        .is_some_and(|(node, query)| !Arc::ptr_eq(node, query))
    {
        return Err(RegistrationError::Options(
            zero_config::ConfigError::InvalidOutbound(
                "actual DNS purposes have forked shared capacity".into(),
            ),
        ));
    }
    if let Some(current) = node.or(query) {
        return Ok(current);
    }
    let mut candidate = network
        .dns_budget
        .lock()
        .expect("DNS candidate budget poisoned");
    if let Some(current) = candidate
        .upgrade()
        .filter(|budget| budget.limits() == limits)
    {
        return Ok(current);
    }
    let budget = Arc::new(Budget::new(limits));
    *candidate = Arc::downgrade(&budget);
    Ok(budget)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Purpose {
    Node,
    Query,
}
impl Purpose {
    fn source(self, network: &BackgroundNetwork) -> &Arc<zero_dns::native::NativeDnsSource> {
        match self {
            Self::Node => &network.native_dns,
            Self::Query => &network.query_dns,
        }
    }
    fn key(self) -> &'static str {
        match self {
            Self::Node => "proxy.dns_node_queries",
            Self::Query => "proxy.dns_queries",
        }
    }
}
pub(crate) struct Options {
    budget: Arc<Budget>,
    purpose: Purpose,
    peers: Vec<(std::net::SocketAddr, bool)>,
    roots: Vec<crate::inventory::service::ServiceRoot>,
    identity: Arc<[u8]>,
    #[cfg(feature = "dns")]
    quic: Vec<std::net::SocketAddr>,
}
pub(crate) fn prepare(
    resources: &mut ResourcePreparation<'_>,
    network: Arc<BackgroundNetwork>,
) -> Result<ComponentId, RegistrationError> {
    prepare_purpose(resources, network, Purpose::Query)
}
pub(crate) fn prepare_node(
    resources: &mut ResourcePreparation<'_>,
    network: Arc<BackgroundNetwork>,
) -> Result<ComponentId, RegistrationError> {
    prepare_purpose(resources, network, Purpose::Node)
}
fn prepare_purpose(
    resources: &mut ResourcePreparation<'_>,
    network: Arc<BackgroundNetwork>,
    purpose: Purpose,
) -> Result<ComponentId, RegistrationError> {
    let candidate = resources.candidate().expect("host cold candidate");
    let runtime = &candidate.runtime;
    let limits = NativeDnsBudgetLimits {
        originals: NonZeroUsize::new(runtime.effective_native_dns_max_originals()).unwrap(),
        queries: NonZeroUsize::new(runtime.effective_native_dns_max_queries()).unwrap(),
        blocking_jobs: NonZeroUsize::new(runtime.effective_native_dns_max_blocking_jobs()).unwrap(),
        h2_drivers: NonZeroUsize::new(runtime.effective_native_dns_max_h2_drivers()).unwrap(),
    };
    let budget = candidate_budget(&network, limits)?;
    let mut configured = candidate.runtime.dns.clone();
    if purpose == Purpose::Node {
        if let Some(dns) = &mut configured {
            if let Some(selected) = &dns.policy.node_server {
                let tags = std::iter::once(selected)
                    .chain(dns.policy.node_fallback_servers.iter())
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>();
                dns.servers.retain(|tag, _| tags.contains(tag));
                dns.default_server = selected.clone();
                dns.dispatch.clear();
                dns.policy.fallback_servers = dns.policy.node_fallback_servers.clone();
            }
        }
    }
    let dns = configured.as_ref();
    let roots = if purpose == Purpose::Query {
        crate::inventory::service::dns_roots(candidate)
    } else {
        Vec::new()
    };
    let peers = zero_dns::native::native_endpoints(dns).map_err(|source| {
        RegistrationError::Options(zero_config::ConfigError::InvalidOutbound(
            source.to_string(),
        ))
    })?;
    static KEYS: OnceLock<[std::collections::hash_map::RandomState; 2]> = OnceLock::new();
    let bytes = serde_json::to_vec(&dns).expect("DNS config serializes");
    let mut identity = Vec::new();
    for key in KEYS.get_or_init(|| [Default::default(), Default::default()]) {
        let mut hash = key.build_hasher();
        hash.write(purpose.key().as_bytes());
        hash.write(format!("{limits:?}").as_bytes());
        hash.write(&bytes);
        hash.write(format!("{roots:?}").as_bytes());
        identity.extend_from_slice(&hash.finish().to_le_bytes());
    }
    resources
        .share(
            purpose.key(),
            Arc::new(Factory(network)),
            Arc::new(Options {
                budget,
                purpose,
                peers,
                roots,
                identity: identity.into(),
                #[cfg(feature = "dns")]
                quic: zero_dns::native::native_quic_endpoints(dns).map_err(|source| {
                    RegistrationError::Options(zero_config::ConfigError::InvalidOutbound(
                        source.to_string(),
                    ))
                })?,
            }),
        )
        .map_err(|source| {
            RegistrationError::Options(zero_config::ConfigError::InvalidOutbound(
                source.to_string(),
            ))
        })
}
struct Factory(Arc<BackgroundNetwork>);
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let limits = options.budget.limits();
        let mut resources = options
            .peers
            .iter()
            .map(
                |(peer, datagram)| ResourceDeclaration::ControlledCarrierPool {
                    endpoint: provider::destination(*peer),
                    dial: Default::default(),
                    datagram: *datagram,
                    max_sockets: limits.originals,
                },
            )
            .collect::<Vec<_>>();
        resources.extend(options.roots.iter().map(|root| {
            ResourceDeclaration::ControlledCarrierPool {
                endpoint: root.endpoint.clone(),
                dial: root.dial.clone(),
                datagram: false,
                max_sockets: limits.originals,
            }
        }));
        // Query jobs, real blocking OS children and persistent H2 drivers
        // share this Scope; unused capacity starts no workers.
        let task_limit =
            limits.queries.get() + limits.blocking_jobs.get() + limits.h2_drivers.get();
        #[cfg(feature = "dns")]
        let (resources, task_limit) = {
            let mut resources = resources;
            let task_limit = task_limit
                + if options.quic.is_empty() {
                    0
                } else {
                    limits.originals.get()
                };
            resources.extend(options.quic.iter().map(|peer| {
                ResourceDeclaration::ControlledMetadataDatagram {
                    endpoint: *peer,
                    dial: Default::default(),
                    limits: provider::quic::metadata_limits(),
                    max_origins: NonZeroUsize::new((limits.originals.get() / 2).max(1)).unwrap(),
                }
            }));
            (resources, task_limit)
        };
        ComponentSpec {
            owner: ResourceOwner::Endpoint(options.purpose.key().into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            task_limit: NonZeroUsize::new(task_limit),
            resources,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let limits = options.budget.limits();
        let owners = (0..options.peers.len() + options.roots.len())
            .map(|_| CarrierOwner::new(context.instance(), limits.originals, &context.cancel))
            .collect::<Vec<_>>();
        #[cfg(feature = "dns")]
        let metadata = options
            .quic
            .iter()
            .map(|_| {
                zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOwner::new(
                    context.instance(),
                    NonZeroUsize::new((limits.originals.get() / 2).max(1)).unwrap(),
                    provider::quic::metadata_limits(),
                    &context.cancel,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let provider = Arc::new(Provider::new(
            &context,
            &options,
            &owners,
            self.0.egress.clone(),
            self.0.clone(),
            #[cfg(feature = "dns")]
            &metadata,
        ));
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                owners,
                provider,
                tasks: context.tasks,
                resources: context.resources,
                network: self.0.clone(),
                purpose: options.purpose,
                pending: CloseReport::default(),
                #[cfg(feature = "dns")]
                metadata,
            }),
        })
    }
}
struct Owner {
    owners: Vec<CarrierOwner>,
    provider: Arc<Provider>,
    tasks: TaskRegistrar,
    resources: Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    network: Arc<BackgroundNetwork>,
    purpose: Purpose,
    pending: CloseReport,
    #[cfg(feature = "dns")]
    metadata: Vec<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOwner>,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), BuildError> {
        for (ordinal, owner) in self.owners.iter_mut().enumerate() {
            self.resources.register_controlled_carrier(
                ordinal,
                owner.registration(),
                &self.tasks,
            )?;
            owner.start(end).await?;
        }
        #[cfg(feature = "dns")]
        for (index, owner) in self.metadata.iter().enumerate() {
            self.resources.register_metadata_pool(
                self.owners.len() + index,
                owner.registration(),
                &self.tasks,
            )?;
            owner.start()?;
        }
        let provider: Arc<dyn zero_dns::native::NativeDnsProvider> = self.provider.clone();
        self.purpose
            .source(&self.network)
            .install(&provider)
            .map_err(provider::error)?;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        let provider: Arc<dyn zero_dns::native::NativeDnsProvider> = self.provider.clone();
        let current = self.purpose.source(&self.network).withdraw(&provider);
        self.provider.stop();
        if current {
            if let Some(dns) = self
                .network
                .resolver
                .get()
                .and_then(std::sync::Weak::upgrade)
            {
                dns.retire_native_query_source(self.purpose.source(&self.network));
            }
        }
        for owner in &self.owners {
            owner.revoke();
        }
        #[cfg(feature = "dns")]
        for owner in &self.metadata {
            owner.revoke();
        }
    }
    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut failure = None;
        if let Some(dns) = self
            .network
            .resolver
            .get()
            .and_then(std::sync::Weak::upgrade)
        {
            if let Err(source) = dns.shutdown_native_pools(&self.provider.tasks(), end).await {
                failure.get_or_insert_with(|| provider::error(source));
            }
        }
        if let Err(source) = self.provider.close_queries(end).await {
            failure.get_or_insert_with(|| provider::error(source));
        }
        let mut unfinished = Vec::new();
        for owner in &mut self.owners {
            match owner.shutdown(end).await {
                Ok(mut report) => {
                    unfinished.append(&mut report.unfinished);
                    self.pending.append(report);
                }
                Err(source) => {
                    failure.get_or_insert(source);
                }
            }
        }
        if let Err(source) = self.provider.reap(end).await {
            failure.get_or_insert_with(|| provider::error(source));
        }
        #[cfg(feature = "dns")]
        for owner in &self.metadata {
            match owner.shutdown() {
                Ok(mut report) => {
                    unfinished.append(&mut report.unfinished);
                    self.pending.append(report);
                }
                Err(source) => {
                    failure.get_or_insert(source);
                }
            }
        }
        if let Some(failure) = failure {
            return Err(failure);
        }
        let mut report = std::mem::take(&mut self.pending);
        report.unfinished = unfinished;
        Ok(report)
    }
}
