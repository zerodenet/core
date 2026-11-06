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
        let base = if options.base.ca_file.is_some() {
            erase_lifecycle(super::super::owner::CaOwner::new(
                owner,
                admission.clone(),
                options.base.clone(),
                prepared.clone(),
                context.resources,
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
struct Owner {
    managed: Arc<Managed>,
    carriers: CarrierOwner,
    service_carriers: Option<CarrierOwner>,
    base: Box<dyn DynResourceLifecycle>,
    attempted: bool,
    resources: Arc<dyn ResourceAccess>,
    carrier_ordinal: usize,
    stream_admission_claimed: bool,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.managed.check().is_ok() {
            return Ok(());
        }
        if self.attempted || self.managed.cancel.is_cancelled() {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        if Instant::now() >= deadline {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Timeout,
            ));
        }
        self.attempted = true;
        if let Some(admission) = &self.managed.options.stream_admission {
            admission.claim_node_streams(self.managed.instance)?;
            self.stream_admission_claimed = true;
        }
        self.managed.options.state.claim(self.managed.instance)?;
        if self.managed.options.business.is_some() {
            self.resources.register_controlled_carrier(
                self.carrier_ordinal,
                self.carriers.registration(),
                &self.managed.tasks,
            )?;
        }
        if let Some(service) = &self.service_carriers {
            self.resources.register_controlled_carrier(
                self.carrier_ordinal + 1,
                service.registration(),
                &self.managed.tasks,
            )?;
        }
        self.base.start(deadline).await?;
        self.carriers.start(deadline).await?;
        if let Some(service) = &mut self.service_carriers {
            service.start(deadline).await?;
        }
        if self.managed.cancel.is_cancelled() || self.managed.tasks.cancellation().is_cancelled() {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        self.managed.ready.store(true, Ordering::Release);
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.managed.stop();
        self.carriers.revoke();
        if let Some(service) = &self.service_carriers {
            service.revoke();
        }
        self.base.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let (base, physical, service) = tokio::join!(
            self.base.shutdown(deadline),
            self.carriers.shutdown(deadline),
            async {
                match &mut self.service_carriers {
                    Some(service) => service.shutdown(deadline).await,
                    None => Ok(CloseReport::default()),
                }
            },
        );
        let records = self.managed.records.lock().unwrap().clone();
        for record in records {
            record.receive_available();
        }
        let mut report = base?;
        report.append(physical?);
        report.append(service?);
        self.managed
            .records
            .lock()
            .unwrap()
            .retain(|record| !record.closed());
        if !self.managed.records.lock().unwrap().is_empty() {
            report.unfinished.push(Unfinished::Io(
                "VMess original gRPC carriers remain unconfirmed".into(),
            ));
        }
        let service_closed = self.service_carriers.as_ref().is_none_or(|owner| {
            owner
                .registration()
                .closed()
                .is_some_and(|closed| closed.matches(&owner.registration()))
        });
        if self.managed.options.node()
            && service_closed
            && report.is_complete()
            && self
                .carriers
                .registration()
                .closed()
                .is_some_and(|closed| closed.matches(&self.carriers.registration()))
        {
            self.managed.options.state.release(self.managed.instance);
            if self.stream_admission_claimed {
                self.managed
                    .options
                    .stream_admission
                    .as_ref()
                    .unwrap()
                    .release_node_streams(self.managed.instance);
                self.stream_admission_claimed = false;
            }
        }
        Ok(report)
    }
}
