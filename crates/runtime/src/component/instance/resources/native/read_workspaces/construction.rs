//! Cold finite declaration validation; no memory, tasks, IO or Root mutation.
use super::*;
/// Validate the finite same-spec reference without allocating or granting I/O.
pub fn packet_datagram_read_spec(
    declarations: &[ResourceDeclaration],
    datagram: usize,
) -> Result<Option<[NonZeroUsize; 2]>, ChannelError> {
    let mut found = None;
    let mut seen = std::collections::BTreeSet::new();
    for declaration in declarations {
        if let ResourceDeclaration::PacketDatagramReads {
            datagram: actual,
            workspaces,
            ..
        } = declaration
        {
            if !matches!(
                declarations.get(*actual),
                Some(
                    ResourceDeclaration::DatagramListener { .. }
                        | ResourceDeclaration::BoundOutboundDatagram { .. }
                )
            ) || !seen.insert(*actual)
            {
                return Err(error(
                    ComponentId(0),
                    Reason::InvalidConfig,
                    "packet reads must reference one distinct original UDP descriptor in the same spec",
                ));
            }
            if *actual == datagram {
                found = Some(*workspaces);
            }
        }
    }
    Ok(found)
}
/// Cold control metadata from the same validated descriptor. No memory/IO grant.
pub fn packet_datagram_control_spec(
    declarations: &[ResourceDeclaration],
    datagram: usize,
) -> Result<Option<NonZeroUsize>, ChannelError> {
    packet_datagram_read_spec(declarations, datagram)?;
    Ok(declarations
        .iter()
        .find_map(|declaration| match declaration {
            ResourceDeclaration::PacketDatagramReads {
                datagram: actual,
                control_workspace,
                ..
            } if *actual == datagram => *control_workspace,
            _ => None,
        }))
}
pub fn packet_datagram_supplemental_spec(
    declarations: &[ResourceDeclaration],
    datagram: usize,
) -> Result<zero_platform_tokio::contract::component::PacketSupplementalMemory, ChannelError> {
    packet_datagram_read_spec(declarations, datagram)?;
    Ok(declarations
        .iter()
        .find_map(|declaration| match declaration {
            ResourceDeclaration::PacketDatagramReads {
                datagram: actual,
                supplemental,
                ..
            } if *actual == datagram => Some(supplemental.clone()),
            _ => None,
        })
        .unwrap_or_default())
}

impl HostPacketReadReservation {
    pub fn new(
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        roots: Arc<crate::HostOriginalBudgets>,
        lengths: [NonZeroUsize; 2],
    ) -> Result<Arc<Self>, ChannelError> {
        Self::new_with_control(instance, scope, roots, lengths, None)
    }
    pub fn new_with_control(
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        roots: Arc<crate::HostOriginalBudgets>,
        lengths: [NonZeroUsize; 2],
        control: Option<NonZeroUsize>,
    ) -> Result<Arc<Self>, ChannelError> {
        Self::new_with_supplemental(instance, scope, roots, lengths, control, Default::default())
    }
    pub fn new_with_supplemental(
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        roots: Arc<crate::HostOriginalBudgets>,
        lengths: [NonZeroUsize; 2],
        control: Option<NonZeroUsize>,
        supplemental: zero_platform_tokio::contract::component::PacketSupplementalMemory,
    ) -> Result<Arc<Self>, ChannelError> {
        if !cfg!(feature = "raw-ip-runtime") {
            return Err(error(
                instance.component,
                Reason::Unsupported,
                "actual packet read memory unavailable",
            ));
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = (roots, lengths, control, supplemental);
        Ok(Arc::new(Self {
            instance,
            scope,
            #[cfg(feature = "raw-ip-runtime")]
            roots,
            #[cfg(feature = "raw-ip-runtime")]
            lengths,
            #[cfg(feature = "raw-ip-runtime")]
            control,
            #[cfg(feature = "raw-ip-runtime")]
            supplemental,
            state: Mutex::new(ReadState::default()),
        }))
    }
}
