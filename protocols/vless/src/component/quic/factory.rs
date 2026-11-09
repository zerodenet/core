use super::*;
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{
        admission::{SessionAdmission, SessionOwner},
        datagram::erase_datagram,
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, Unfinished},
        stream::erase_stream,
        CloseReason,
    },
    controlled_carrier::CarrierOwner,
    controlled_datagram::metadata_pool::MetadataPoolOwner,
};
mod failures;
pub struct Factory;
pub(super) struct Outbound {
    pub generation: PacketMetadataArc<entry::Generation>,
    pub admission: Arc<SessionAdmission>,
}
struct Owner {
    session: SessionOwner,
    generation: PacketMetadataArc<entry::Generation>,
    carrier: CarrierOwner,
    started: bool,
    failures: failures::OriginalSources,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut resources = vec![
            ResourceDeclaration::ControlledMetadataCarrier {
                endpoint: options.protocol.endpoint.clone(),
                dial: options.protocol.dial.clone(),
                ports: vec![],
                limits: options.limits,
                max_origins: options.origins,
            },
            ResourceDeclaration::ControlledServiceDatagram {
                endpoint: options.protocol.endpoint.clone(),
                dial: options.protocol.dial.clone(),
                max_sockets: options.origins,
            },
        ];
        resources.extend(
            options
                .protocol
                .material
                .as_ref()
                .and_then(|material| material.resource()),
        );
        if options.requires_icmp() {
            resources.push(ResourceDeclaration::NativeIcmp {
                max_sockets: options.origins,
            });
        }
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
            dependencies: options.dependencies.clone(),
            static_identity: options.identity.clone(),
            resources,
            task_limit: Some(n(options
                .origins
                .get()
                .checked_mul(
                    4 + zero_transport::finalmask::owned::driver_count(
                        options.plan.owned_quic_masks(),
                    ),
                )
                .expect("validated task peak"))),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_with_ech_resolver(options, context, None)
    }
}
impl Factory {
    pub fn build_with_ech_resolver(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        let (session, admission) = SessionOwner::new(context.component, &context.cancel);
        let buffers = context
            .resources
            .carrier_buffer_budget()
            .ok_or_else(|| error(Stage::Prepare, Reason::Unsupported))?;
        let metadata_memory = context.resources.packet_driver_metadata()?;
        let entries = entry::Table::new(&metadata_memory, options.origins.get())?;
        let instance = context.instance();
        let cleanup = zero_platform_tokio::contract::CancellationToken::new();
        let upstream = context
            .background_carrier
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        let carrier = CarrierOwner::new(instance, options.origins, &cleanup);
        let metadata = MetadataPoolOwner::new(instance, options.origins, options.limits, &cleanup)?;
        if buffers.limits() != options.buffers {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let failures = failures::OriginalSources::prepare(&metadata_memory)?;
        let generation_memory = metadata_memory
            .reserve(
                PacketMetadataArc::<entry::Generation>::layout().map_err(|source| {
                    error(Stage::Prepare, Reason::Capacity).with_source(source)
                })?,
            )
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let generation = PacketMetadataArc::from_prepaid(
            entry::Generation {
                instance,
                cleanup,
                metadata,
                carrier: carrier.grants(),
                resources: context.resources.clone(),
                tasks: context.tasks,
                upstream,
                options,
                cancel: context.cancel.child_token(),
                buffers,
                metadata_memory,
                resolver,
                entries: Mutex::new(entries),
            },
            generation_memory,
        )
        .map_err(|(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                session,
                generation: generation.clone(),
                carrier,
                started: false,
                failures,
            }),
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound {
                    generation: generation.clone(),
                    admission: admission.clone(),
                })),
                datagram: generation.options.protocol.flow.is_none().then(|| {
                    erase_datagram(Outbound {
                        generation,
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
        if self.started {
            return self.session.start(end).await;
        }
        self.started = true;
        let generation = &self.generation;
        generation.resources.register_metadata_pool(
            0,
            generation.metadata.registration(),
            &generation.tasks,
        )?;
        generation.resources.register_controlled_carrier(
            1,
            self.carrier.registration(),
            &generation.tasks,
        )?;
        if let Some(material) = &generation.options.protocol.material {
            material.record_bound(generation.instance, generation.resolver.as_ref())?;
            material
                .start(
                    generation.resources.as_ref(),
                    generation.instance,
                    &generation.cancel,
                    end,
                )
                .await?;
        }
        generation.metadata.start()?;
        self.carrier.start(end).await?;
        self.session.start(end).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.session.revoke(reason);
        self.generation.cancel.cancel();
        // Neither a slot cancel nor CarrierOwner.revoke may cut off a final
        // accepted queue. Installed Root phases own those cleanup futures.
        self.generation.request_retirement();
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        // Existing Root pollers drive all rows concurrently. This observer does
        // not run another close future or wait any actor from its own body.
        let waiting = async {
            let mut previous = 0;
            while let Some((ordinal, entry)) = self.generation.next_entry(previous) {
                previous = ordinal;
                entry.retirement.wait_protocol_original().await?;
            }
            Ok::<_, ChannelError>(())
        };
        match tokio::time::timeout_at(end, waiting).await {
            Ok(result) => result?,
            Err(_) => {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(
                        "actual QUIC protocol cleanup remains Root-held".into(),
                    )],
                    ..Default::default()
                })
            }
        }
        // The original phase returned unique views. Ordinary original close
        // now supplies native/users proofs and checks actual registration ACKs.
        let (metadata, carrier) = self.failures.branches();
        tokio::join!(
            async {
                metadata.capture(
                    self.generation
                        .metadata
                        .shutdown_after_registration_ack(end)
                        .await,
                )
            },
            async { carrier.capture(self.carrier.shutdown(end).await) }
        );
        let mut report = CloseReport::default();
        self.failures.report(&self.generation, &mut report)?;
        self.generation.reap();
        report.append(self.session.shutdown(end).await?);
        if report.is_complete() {
            self.generation.cleanup.cancel();
        }
        Ok(report)
    }
}
