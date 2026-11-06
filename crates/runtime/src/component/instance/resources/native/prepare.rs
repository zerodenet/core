//! Cold construction installs declaration bindings before lifecycle start.
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
        Self::construct(component, declarations, cancel, None, None)
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
        )
    }
    pub(in crate::component) fn new_for_instance_with_packet_source(
        instance: ComponentInstanceId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
        scope: TaskScopeIdentity,
        source: Arc<
            std::sync::OnceLock<
                zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
            >,
        >,
    ) -> Result<Arc<Self>, ChannelError> {
        Self::construct(
            instance.component,
            declarations,
            cancel,
            Some((instance, scope)),
            Some(source),
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
    ) -> Result<Arc<Self>, ChannelError> {
        let bounds = bound_datagram::BoundDeclarations::new(
            component,
            declarations,
            identity.clone(),
            cancel.clone(),
        )?;
        #[cfg(target_os = "linux")]
        let processes = process::ProcessDeclarations::new(declarations, identity.clone());
        let roots =
            root_datagram::RootDeclarations::new(component, declarations, identity.clone())?;
        let metadata =
            metadata::MetadataDeclarations::new(component, declarations, identity.clone())?;
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
                    icmp = Some(super::super::icmp::HostIcmpProbeOwner::new_inbound_carrier(
                        *instance,
                        declaration,
                        cancel.clone(),
                    )?);
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
                    let owner = sockets::TcpGrant {
                        native: zero_platform_tokio::ControlledTcpListenerOwner::new(
                            declaration,
                            *max_bind_attempts,
                            cancel.clone(),
                        )?,
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
                    let owner = sockets::DatagramGrant::new(
                        component,
                        *max_bind_attempts,
                        *max_pending_sends,
                    )?;
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
                ResourceDeclaration::BoundOutboundDatagram { .. }
                | ResourceDeclaration::ControlledOutboundDatagram { .. }
                | ResourceDeclaration::ControlledOutboundStream { .. }
                | ResourceDeclaration::ControlledCarrierPool { .. }
                | ResourceDeclaration::ControlledServiceStream { .. }
                | ResourceDeclaration::ControlledAssociationPool { .. }
                | ResourceDeclaration::ControlledMetadataDatagram { .. }
                | ResourceDeclaration::ControlledMetadataCarrier { .. }
                | ResourceDeclaration::ControlledDatagramCarrier { .. }
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
            packet_source: packet_source.unwrap_or_default(),
            component,
            icmp,
            coverage,
            carriers,
            metadata,
            metapools,
            roots,
            bounds,
            packets,
            #[cfg(target_os = "linux")]
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
