//! Actual dependency, task Scope and original owner registered before first assignment.
use super::*;
use tokio::time::Instant;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::{
        admission::{SessionAdmission, SessionOwner},
        lifecycle::{erase_lifecycle, BuildError, ResourceLifecycle},
        scope::TaskRegistrar,
    },
    controlled_carrier::{CarrierBufferBudget, CarrierGrants, CarrierOwner},
};
pub struct Factory;
pub(super) struct Shared {
    pub options: Arc<Options>,
    pub dependency: ComponentInstanceId,
    pub resources: Arc<dyn ResourceAccess>,
    pub tasks: TaskRegistrar,
    pub grants: CarrierGrants,
    pub upstream: zero_platform_tokio::contract::network::UpstreamTransport,
    pub buffers: CarrierBufferBudget,
    pub memory: PacketDriverMetadataView,
    pub entries: PacketMetadataArc<Mutex<table::Table>>,
    pub cancel: CancellationToken,
}
pub(super) struct Outbound {
    pub shared: PacketMetadataArc<Shared>,
    pub admission: Arc<SessionAdmission>,
}
struct Owner {
    shared: PacketMetadataArc<Shared>,
    carrier: CarrierOwner,
    session: SessionOwner,
    registered: bool,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: std::iter::once(RoleKind::Stream)
                .chain(
                    options
                        .protocol
                        .flow
                        .is_none()
                        .then_some(RoleKind::Datagram),
                )
                .collect(),
            dependencies: options.dependency.into_iter().collect(),
            static_identity: options.identity.clone(),
            resources: vec![ResourceDeclaration::ControlledCarrierPool {
                endpoint: options.protocol.endpoint.clone(),
                dial: options.protocol.dial.clone(),
                max_sockets: options.origins,
                datagram: false,
            }],
            task_limit: Some(n(options
                .origins
                .get()
                .checked_mul(2)
                .expect("validated Browser task peak"))),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let dependency = options
            .dependency
            .and_then(|id| context.dependency(id))
            .filter(|_| context.dependencies.len() == 1)
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        let buffers = context
            .resources
            .carrier_buffer_budget()
            .ok_or_else(|| error(Stage::Prepare, Reason::Unsupported))?;
        let memory = context.resources.packet_driver_metadata()?;
        let upstream = context
            .background_carrier
            .clone()
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        let entries = table::Table::prepare(&memory, options.origins.get())?;
        let header = memory
            .reserve(PacketMetadataArc::<Shared>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        let carrier = CarrierOwner::new(context.instance(), options.origins, &context.cancel);
        let (session, admission) = SessionOwner::new(context.component, &context.cancel);
        let shared = PacketMetadataArc::from_prepaid(
            Shared {
                options,
                dependency,
                upstream,
                resources: context.resources,
                tasks: context.tasks,
                grants: carrier.grants(),
                buffers,
                memory,
                entries,
                cancel: context.cancel.child_token(),
            },
            header,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                shared: shared.clone(),
                carrier,
                session,
                registered: false,
            }),
            roles: ComponentRoles {
                stream: Some(zero_platform_tokio::contract::stream::erase_stream(
                    Outbound {
                        shared: shared.clone(),
                        admission: admission.clone(),
                    },
                )),
                datagram: shared.options.protocol.flow.is_none().then(|| {
                    zero_platform_tokio::contract::datagram::erase_datagram(Outbound {
                        shared,
                        admission,
                    })
                }),
                ..Default::default()
            },
        })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        if !self.registered {
            self.shared.resources.register_controlled_carrier(
                0,
                self.carrier.registration(),
                &self.shared.tasks,
            )?;
            self.registered = true;
        }
        // Dependency start order already established the one real Source.
        self.shared
            .options
            .provider
            .dialer(self.shared.dependency)?;
        self.carrier.start(end).await?;
        self.session.start(end).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.session.revoke(reason);
        self.shared.cancel.cancel();
        table::Table::request_retirement(&self.shared.entries);
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let work = async {
            let mut after = 0;
            while let Some((ordinal, entry)) = table::Table::next(&self.shared.entries, after) {
                after = ordinal;
                if let Some(original) = entry.retirement.get() {
                    original.wait_protocol_original().await?;
                }
            }
            Ok::<_, ChannelError>(())
        };
        match tokio::time::timeout_at(end, work).await {
            Ok(result) => result?,
            Err(_) => {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(
                        "same Browser setup/driver/Source originals remain Root-held".into(),
                    )],
                    ..Default::default()
                })
            }
        }
        let mut report = self.carrier.shutdown(end).await?;
        report.append(self.session.shutdown(end).await?);
        table::Table::reap(&self.shared.entries);
        Ok(report)
    }
}
