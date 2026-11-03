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
pub(crate) const LIMIT: usize = 16;
pub(crate) struct Options {
    peers: Vec<(std::net::SocketAddr, bool)>,
    identity: Arc<[u8]>,
    #[cfg(feature = "dns")]
    quic: Vec<std::net::SocketAddr>,
}
pub(crate) fn prepare(
    resources: &mut ResourcePreparation<'_>,
    network: Arc<BackgroundNetwork>,
) -> Result<ComponentId, RegistrationError> {
    let candidate = resources.candidate().expect("host cold candidate");
    let dns = candidate.runtime.dns.as_ref();
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
        hash.write(&bytes);
        identity.extend_from_slice(&hash.finish().to_le_bytes());
    }
    resources
        .share(
            "proxy.dns_queries",
            Arc::new(Factory(network)),
            Arc::new(Options {
                peers,
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
        let resources = options
            .peers
            .iter()
            .map(
                |(peer, datagram)| ResourceDeclaration::ControlledCarrierPool {
                    endpoint: provider::destination(*peer),
                    dial: Default::default(),
                    datagram: *datagram,
                    max_sockets: NonZeroUsize::new(LIMIT).unwrap(),
                },
            )
            .collect::<Vec<_>>();
        let task_limit = LIMIT;
        #[cfg(feature = "dns")]
        let (resources, task_limit) = {
            let mut resources = resources;
            let task_limit = task_limit + if options.quic.is_empty() { 0 } else { LIMIT };
            resources.extend(options.quic.iter().map(|peer| {
                ResourceDeclaration::ControlledMetadataDatagram {
                    endpoint: *peer,
                    dial: Default::default(),
                    limits: provider::quic::metadata_limits(),
                    max_origins: NonZeroUsize::new(LIMIT / 2).unwrap(),
                }
            }));
            (resources, task_limit)
        };
        ComponentSpec {
            owner: ResourceOwner::Endpoint("dns.queries".into()),
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
        let owners = options
            .peers
            .iter()
            .map(|_| {
                CarrierOwner::new(
                    context.instance(),
                    NonZeroUsize::new(LIMIT).unwrap(),
                    &context.cancel,
                )
            })
            .collect::<Vec<_>>();
        #[cfg(feature = "dns")]
        let metadata = options
            .quic
            .iter()
            .map(|_| {
                zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOwner::new(
                    context.instance(),
                    NonZeroUsize::new(LIMIT / 2).unwrap(),
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
        self.network
            .native_dns
            .install(&provider)
            .map_err(provider::error)?;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        let provider: Arc<dyn zero_dns::native::NativeDnsProvider> = self.provider.clone();
        let current = self.network.native_dns.withdraw(&provider);
        self.provider.stop();
        if current {
            if let Some(dns) = self
                .network
                .resolver
                .get()
                .and_then(std::sync::Weak::upgrade)
            {
                dns.retire_native_queries();
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
        let mut failure = self
            .provider
            .close_queries(end)
            .await
            .err()
            .map(provider::error);
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
