//! One actual DNS purpose factory; all protocols share its original budgets.
use super::*;
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
        resources.extend(options.ech.iter().map(|endpoint| {
            ResourceDeclaration::ControlledCarrierPool {
                endpoint: ech::destination(endpoint),
                dial: Default::default(),
                datagram: endpoint.is_datagram(),
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
                    limits: options.quic_metadata,
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
        let owners = (0..options.peers.len() + options.roots.len() + options.ech.len())
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
                    options.quic_metadata,
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
                reported_faults: Default::default(),
                #[cfg(feature = "dns")]
                metadata,
            }),
        })
    }
}
