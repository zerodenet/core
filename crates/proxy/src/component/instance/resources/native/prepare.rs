//! Cold construction installs declaration bindings before lifecycle start.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::scope::TaskScopeIdentity;
impl HostResources {
    #[cfg(test)]
    pub(in crate::component) fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, ChannelError> {
        Self::construct(component, declarations, cancel, None)
    }
    pub(in crate::component) fn new_for_instance(
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
        )
    }
    fn construct(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
        identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
    ) -> Result<Arc<Self>, ChannelError> {
        let roots =
            root_datagram::RootDeclarations::new(component, declarations, identity.clone())?;
        let metadata =
            metadata::MetadataDeclarations::new(component, declarations, identity.clone())?;
        let carriers =
            carrier::CarrierDeclarations::new(component, declarations, identity, cancel.clone())?;
        let mut remaining = BTreeMap::new();
        let mut tcp = BTreeMap::new();
        let mut datagram = BTreeMap::new();
        let mut files = files::FileReads::default();
        for declaration in declarations {
            match declaration {
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
                        return Err(error(component, Reason::Unsupported,
                            "accepted TCP authority requires the actual host handler Scope provider"));
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
                ResourceDeclaration::ControlledOutboundDatagram { .. }
                | ResourceDeclaration::ControlledOutboundStream { .. }
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
                    ))
                }
            }
        }
        let coverage = declarations
            .iter()
            .map(|declaration| coverage::CoverageBinding::new(component, declaration))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Self {
            component,
            coverage,
            carriers,
            metadata,
            roots,
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
