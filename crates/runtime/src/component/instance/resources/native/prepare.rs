//! Cold construction installs declaration bindings before lifecycle start.
mod hosted;
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::scope::TaskScopeIdentity;
impl HostResources {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, ChannelError> {
        Self::construct(
            component,
            declarations,
            cancel,
            None,
            None,
            Arc::default(),
            None,
        )
    }
    pub fn new_for_instance(
        instance: ComponentInstanceId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
        scope: TaskScopeIdentity,
    ) -> Result<Arc<Self>, ChannelError> {
        Self::construct(
            instance.component,
            declarations,
            cancel,
            Some((instance, scope)),
            None,
            Arc::default(),
            None,
        )
    }
    fn construct(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
        identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
        packet_source: Option<
            Arc<
                std::sync::OnceLock<
                    zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
                >,
            >,
        >,
        originals: Arc<crate::HostOriginalBudgets>,
        original_tasks: Option<zero_platform_tokio::contract::scope::WeakTaskRegistrar>,
    ) -> Result<Arc<Self>, ChannelError> {
        let reads = read_workspaces::ReadDeclarations::new(
            component,
            declarations,
            identity.clone(),
            originals.clone(),
        )?;
        let bounds = bound_datagram::BoundDeclarations::new(
            component,
            declarations,
            identity.clone(),
            cancel.clone(),
            originals.carrier.clone(),
        )?;
        #[cfg(any(target_os = "linux", windows))]
        let processes = process::ProcessDeclarations::new(
            declarations,
            identity.clone(),
            originals.process.clone(),
        )?;
        let roots =
            root_datagram::RootDeclarations::new(component, declarations, identity.clone())?;
        let metadata = metadata::MetadataDeclarations::new(
            component,
            declarations,
            identity.clone(),
            &originals,
            original_tasks.as_ref(),
            &cancel,
        )?;
        let carriers = carrier::CarrierDeclarations::new(
            component,
            declarations,
            identity.clone(),
            cancel.clone(),
        )?;
        let metapools = metapool::MetadataPools::new(
            component,
            declarations,
            identity.clone(),
            cancel.clone(),
        )?;
        let mut packets = Vec::new();
        let mut icmp = None;
        let mut remaining = BTreeMap::new();
        let mut tcp = BTreeMap::new();
        let mut datagram = BTreeMap::new();
        let mut files = files::FileReads::default();
        for (ordinal, declaration) in declarations.iter().enumerate() {
            match declaration {
                ResourceDeclaration::ControlledPacketDevice { name, binding } => {
                    if packets
                        .iter()
                        .any(|owner: &packet_device::PacketDeviceOwner| {
                            owner.name() == name.as_ref()
                        })
                    {
                        return Err(error(
                            component,
                            Reason::InvalidConfig,
                            "duplicate Packet device declaration",
                        ));
                    }
                    let (instance, scope) = identity.as_ref().ok_or_else(|| {
                        error(
                            component,
                            Reason::InvalidConfig,
                            "Packet device requires its actual instance and Scope",
                        )
                    })?;
                    packets.push(packet_device::PacketDeviceOwner::new(
                        ordinal,
                        *instance,
                        scope.clone(),
                        name.clone(),
                        binding.clone(),
                        cancel.clone(),
                        originals.clone(),
                        original_tasks.clone(),
                    )?);
                }

                ResourceDeclaration::NativeIcmp { .. } => {
                    if icmp.is_some() {
                        return Err(error(
                            component,
                            Reason::InvalidConfig,
                            "duplicate native ICMP declaration",
                        ));
                    }
                    let (instance, _) = identity.as_ref().ok_or_else(|| {
                        error(
                            component,
                            Reason::InvalidConfig,
                            "ICMP requires an actual instance and Scope",
                        )
                    })?;
                    let owner = super::super::icmp::HostIcmpProbeOwner::new_inbound_carrier(
                        *instance,
                        declaration,
                        cancel.clone(),
                    )?;
                    if let Some(tasks) = &original_tasks {
                        let scope = tasks.retirement_observation().ok_or_else(|| {
                            error(
                                component,
                                Reason::Revoked,
                                "ICMP original Source Scope expired",
                            )
                        })?;
                        owner.bind_original_budgets(&originals.carrier, scope)?;
                    }
                    icmp = Some(owner);
                }

                ResourceDeclaration::ProcessEndpoint { .. } => {
                    #[cfg(not(any(target_os = "linux", windows)))]
                    return Err(error(
                        component,
                        Reason::Unsupported,
                        "process endpoint native provider is unavailable",
                    ));
                }
                ResourceDeclaration::Process {
                    program,
                    max_starts,
                } => {
                    if program.as_os_str().is_empty()
                        || remaining
                            .insert(program.clone(), max_starts.get())
                            .is_some()
                    {
                        return Err(error(
                            component,
                            Reason::InvalidConfig,
                            "empty or duplicate process declaration",
                        ));
                    }
                }
                ResourceDeclaration::TcpListener {
                    destination,
                    max_bind_attempts,
                    accepted,
                } => {
                    if accepted.is_some() {
                        return Err(error(
                            component,
                            Reason::Unsupported,
                            "accepted TCP authority requires the actual host handler Scope provider",
                        ));
                    }
                    let address = sockets::literal(component, destination)?;
                    let mut native = zero_platform_tokio::ControlledTcpListenerOwner::new(
                        declaration,
                        *max_bind_attempts,
                        cancel.clone(),
                    )?;
                    if let Some((instance, _)) = &identity {
                        if let Some(tasks) = &original_tasks {
                            let scope = tasks.retirement_observation().ok_or_else(|| {
                                error(
                                    component,
                                    Reason::Revoked,
                                    "actual listener Scope disappeared before root binding",
                                )
                            })?;
                            native.bind_original_totals_observed(
                                *instance,
                                scope,
                                &originals.carrier,
                            )?;
                        }
                    }
                    let owner = sockets::TcpGrant {
                        native,
                        acquired: false,
                        attempted: false,
                    };
                    if tcp
                        .insert(address, Arc::new(AsyncMutex::new(owner)))
                        .is_some()
                    {
                        return Err(error(
                            component,
                            Reason::InvalidConfig,
                            "duplicate listener declaration",
                        ));
                    }
                }
                ResourceDeclaration::DatagramListener {
                    destination,
                    max_bind_attempts,
                    max_pending_sends,
                    ..
                } => {
                    let address = sockets::literal(component, destination)?;
                    let mut owner = sockets::DatagramGrant::new(
                        component,
                        *max_bind_attempts,
                        *max_pending_sends,
                    )?;
                    if let (Some((instance, _)), Some(tasks)) = (&identity, &original_tasks) {
                        let scope = tasks.retirement_observation().ok_or_else(|| {
                            error(
                                component,
                                Reason::Revoked,
                                "actual UDP listener Scope disappeared",
                            )
                        })?;
                        owner.bind_source_totals(zero_platform_tokio::controlled_datagram::SourceDatagramTotalsBinding::new(&originals.carrier,*instance,scope,cancel.clone()))?;
                    }
                    if datagram
                        .insert(address, Arc::new(AsyncMutex::new(owner)))
                        .is_some()
                    {
                        return Err(error(
                            component,
                            Reason::InvalidConfig,
                            "duplicate listener declaration",
                        ));
                    }
                }
                ResourceDeclaration::File { path, max_bytes } => {
                    files.declare(component, path, *max_bytes)?
                }
                ResourceDeclaration::PacketDatagramReads { .. }
                | ResourceDeclaration::BoundOutboundDatagram { .. }
                | ResourceDeclaration::ControlledOutboundDatagram { .. }
                | ResourceDeclaration::ControlledOutboundStream { .. }
                | ResourceDeclaration::ControlledCarrierPool { .. }
                | ResourceDeclaration::ControlledServiceStream { .. }
                | ResourceDeclaration::ControlledAssociationPool { .. }
                | ResourceDeclaration::ControlledMetadataDatagram { .. }
                | ResourceDeclaration::ControlledMetadataCarrier { .. }
                | ResourceDeclaration::ControlledDatagramCarrier { .. }
                | ResourceDeclaration::ControlledNodeCarrier { .. }
                | ResourceDeclaration::ControlledNodeStream { .. }
                | ResourceDeclaration::ControlledDirectStream { .. }
                | ResourceDeclaration::MetadataDatagram { .. } => {}
                ResourceDeclaration::OutboundDatagram { .. } => {
                    return Err(error(
                        component,
                        Reason::Unsupported,
                        "outbound datagram access requires its installed dynamic host owner",
                    ));
                }
                _ => {
                    return Err(error(
                        component,
                        Reason::InvalidConfig,
                        "resource grant is not implemented",
                    ));
                }
            }
        }
        let coverage = declarations
            .iter()
            .map(|declaration| coverage::CoverageBinding::new(component, declaration))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Self {
            source_pause: std::sync::OnceLock::new(),
            originals,
            original_tasks,
            packet_source: packet_source.unwrap_or_default(),
            component,
            icmp,
            coverage,
            carriers,
            metadata,
            metapools,
            roots,
            bounds,
            reads,
            packets,
            #[cfg(any(target_os = "linux", windows))]
            processes,
            cancel,
            state: Mutex::new(State {
                closed: false,
                start_deadline: None,
                remaining,
                tcp,
                datagram,
                files,
                io: shutdown::NativeClose::default(),
            }),
            changed: Arc::new(tokio::sync::Notify::new()),
            cleanup: AsyncMutex::new(()),
        }))
    }
}
