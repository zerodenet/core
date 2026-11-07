//! One finite entry per explicit same-spec supplemental read declaration.
use super::*;
use zero_platform_tokio::contract::scope::WeakTaskRegistrar;
struct Entry {
    ordinal: usize,
    datagram: usize,
    destination: Option<Destination>,
    original: Arc<HostPacketReadReservation>,
}
pub(in crate::component::instance::resources::native) struct ReadDeclarations {
    component: ComponentId,
    entries: Vec<Entry>,
}
#[derive(Clone, Debug)]
pub(in crate::component) struct ReadEvidence {
    pub ordinal: usize,
    pub released: bool,
}
impl ReadDeclarations {
    pub(in crate::component::instance::resources::native) fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
        roots: Arc<crate::HostOriginalBudgets>,
    ) -> Result<Self, ChannelError> {
        packet_datagram_read_spec(declarations, usize::MAX)?;
        let mut entries = Vec::new();
        for (ordinal, declaration) in declarations.iter().enumerate() {
            let ResourceDeclaration::PacketDatagramReads {
                datagram,
                workspaces,
                control_workspace,
            } = declaration
            else {
                continue;
            };
            let (instance, scope) = identity.as_ref().ok_or_else(|| {
                error(
                    component,
                    Reason::InvalidConfig,
                    "read memory requires its actual instance and Scope",
                )
            })?;
            entries.push(Entry {
                ordinal,
                datagram: *datagram,
                destination: match &declarations[*datagram] {
                    ResourceDeclaration::DatagramListener { destination, .. } => {
                        Some(destination.clone())
                    }
                    _ => None,
                },
                original: HostPacketReadReservation::new_with_control(
                    *instance,
                    scope.clone(),
                    roots.clone(),
                    *workspaces,
                    *control_workspace,
                )?,
            });
        }
        Ok(Self { component, entries })
    }
    pub(in crate::component::instance::resources::native) fn prepare_driver(
        &self,
        datagram: usize,
        driver: &TaskJoinWitness,
    ) -> Result<(), ChannelError> {
        if let Some(entry) = self.entries.iter().find(|entry| entry.datagram == datagram) {
            entry.original.prepare_for_driver(driver)?;
        }
        Ok(())
    }
    pub(in crate::component::instance::resources::native) fn prepare_listener(
        &self,
        destination: &Destination,
        tasks: Option<&WeakTaskRegistrar>,
    ) -> Result<(), ChannelError> {
        if let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.destination.as_ref() == Some(destination))
        {
            let tasks = tasks.ok_or_else(|| {
                error(
                    self.component,
                    Reason::Revoked,
                    "actual listener read registrar missing",
                )
            })?;
            entry.original.prepare_for_weak_scope(tasks)?;
        }
        Ok(())
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::component::instance::resources::native) fn take(
        &self,
        datagram: usize,
    ) -> Result<[zero_tun::packet_memory::NativeReadGrant; 2], ChannelError> {
        self.entries
            .iter()
            .find(|entry| entry.datagram == datagram)
            .ok_or_else(|| {
                error(
                    self.component,
                    Reason::InvalidConfig,
                    "no original packet read declaration",
                )
            })?
            .original
            .take()
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::component::instance::resources::native) fn take_control(
        &self,
        datagram: usize,
    ) -> Result<Option<zero_tun::packet_memory::NativeReadGrant>, ChannelError> {
        self.entries
            .iter()
            .find(|entry| entry.datagram == datagram)
            .ok_or_else(|| {
                error(
                    self.component,
                    Reason::InvalidConfig,
                    "no original packet read declaration",
                )
            })?
            .original
            .take_control()
    }
    pub(in crate::component::instance::resources::native) fn revoke(&self) {
        for entry in &self.entries {
            entry.original.revoke();
        }
    }
    pub(in crate::component::instance::resources::native) fn evidence(&self) -> Vec<ReadEvidence> {
        self.entries
            .iter()
            .map(|entry| ReadEvidence {
                ordinal: entry.ordinal,
                released: entry.original.released(),
            })
            .collect()
    }
}
