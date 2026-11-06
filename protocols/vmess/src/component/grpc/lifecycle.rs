use super::{state::Managed, *};
use std::sync::atomic::{AtomicBool, Ordering};
use zero_platform_tokio::{
    contract::{
        admission::SessionOwner,
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ControlledStreamDemandClass, ResourceAccess, ResourceDeclaration, RoleKind,
        },
        lifecycle::{
            erase_lifecycle, BuildError, CloseReport, DynResourceLifecycle, ResourceLifecycle,
            Unfinished,
        },
        stream::erase_stream,
        CloseReason,
    },
    controlled_carrier::CarrierOwner,
};
pub struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = super::super::Factory.describe(&options.base);
        spec.roles = if options.node_streams {
            vec![RoleKind::Stream, RoleKind::Datagram]
        } else if options.node() {
            vec![RoleKind::Datagram]
        } else {
            vec![RoleKind::Stream]
        };
        spec.dependencies = options.dns_dependency.into_iter().collect();
        spec.static_identity = options.identity.clone();
        spec.resources.push(match &options.business {
            Some(_) if options.node() => ResourceDeclaration::ControlledNodeStream {
                endpoint: options.base.endpoint.clone(),
                dial: options.base.dial.clone(),
                max_sockets: options.cap,
            },
            Some(business) => ResourceDeclaration::ControlledOutboundStream {
                endpoint: options.base.endpoint.clone(),
                binding: business.binding.clone().expect("fixed business binding"),
                demand: ControlledStreamDemandClass::Business,
                max_sockets: options.cap,
            },
            None => ResourceDeclaration::BackgroundCarrier,
        });
        if options.node_streams {
            // Per-connection Service authority shares this exact records/N/
            // Scope. Its real driver is not a persistent cached-pool handoff;
            // the awaiting caller retains cleanup through actual loan delivery.
            spec.resources
                .push(ResourceDeclaration::ControlledServiceStream {
                    endpoint: options.base.endpoint.clone(),
                    dial: options.base.dial.clone(),
                    max_sockets: options.cap,
                });
        }
        spec.task_limit = Some(options.cap);
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, None)
    }
}
impl Factory {
    pub fn build_with_ech_resolver(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        resolver: Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, Some(resolver))
    }
    fn build_owned(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        let valid_dependencies = match options.dns_dependency {
            Some(id) => {
                context.dependencies.len() == 1
                    && context
                        .dependency(id)
                        .is_some_and(|dep| dep.component != context.component)
            }
            None => {
                context.dependencies.is_empty()
                    && !matches!(options.base.endpoint.address, zero_core::Address::Domain(_))
            }
        };
        if !valid_dependencies || context.roles.outbound.is_none() {
            return Err(error(instance, Stage::Prepare, Reason::InvalidConfig));
        }
        let upstream = if options.business.is_some() {
            if context.background_carrier.is_some() {
                return Err(error(instance, Stage::Prepare, Reason::InvalidConfig));
            }
            None
        } else {
            Some(
                context
                    .background_carrier
                    .clone()
                    .filter(|upstream| {
                        upstream.binding().prefix.is_empty()
                            && upstream.binding().dial == options.base.dial
                    })
                    .ok_or_else(|| error(instance, Stage::Prepare, Reason::InvalidConfig))?,
            )
        };
        let resources = context.resources.clone();
        // The original File declaration, when present, remains ordinal zero.
        let carrier_ordinal = usize::from(options.base.ca_file.is_some());
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        let prepared = Arc::new(Mutex::new(None));
        let base = if options.base.ca_file.is_some() || options.base.material.is_some() {
            erase_lifecycle(super::super::owner::CaOwner::new(
                owner,
                admission.clone(),
                options.base.clone(),
                prepared.clone(),
                context.resources,
                instance,
                context.cancel.clone(),
            ))
        } else {
            erase_lifecycle(owner)
        };
        let cancel = context.cancel.child_token();
        let carriers = CarrierOwner::new(instance, options.cap, &cancel);
        let service_carriers = options
            .node_streams
            .then(|| CarrierOwner::new(instance, options.cap, &cancel));
        let native_budget = Arc::new(tokio::sync::Semaphore::new(options.cap.get()));
        let managed = Arc::new(Managed {
            instance,
            options,
            admission,
            prepared,
            ech_resolver: resolver,
            upstream,
            tasks: context.tasks,
            cancel,
            ready: AtomicBool::new(false),
            grants: carriers.grants(),
            service_grants: service_carriers.as_ref().map(CarrierOwner::grants),
            records: Mutex::new(Vec::new()),
            native_budget,
            network: tokio::sync::Mutex::new(None),
        });
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: (!managed.options.node() || managed.options.node_streams)
                    .then(|| erase_stream(super::connect::Outbound(managed.clone()))),
                datagram: managed.options.node().then(|| {
                    zero_platform_tokio::contract::datagram::erase_datagram(
                        super::connect::Outbound(managed.clone()),
                    )
                }),
                ..Default::default()
            },
            lifecycle: erase_lifecycle(Owner {
                managed,
                carriers,
                service_carriers,
                base,
                attempted: false,
                resources,
                carrier_ordinal,
                stream_admission_claimed: false,
            }),
        })
    }
}

mod owner;
use owner::Owner;
