//! Same-Host pure software memory. No endpoint, socket or provider is invented.
use super::*;
#[cfg(feature = "raw-ip-runtime")]
mod registration;
#[cfg(feature = "raw-ip-runtime")]
use zero_platform_tokio::contract::scope::TaskJoinWitness;
#[cfg(feature = "raw-ip-runtime")]
use zero_tun::packet_memory::{PacketMetadataArc, PacketMetadataVec};
#[cfg(feature = "raw-ip-runtime")]
struct Entry {
    ordinal: usize,
    original: PacketMetadataArc<HostPacketReadReservation>,
}
pub(super) struct LogicalWorkspaces {
    #[cfg(feature = "raw-ip-runtime")]
    component: ComponentId,
    #[cfg(feature = "raw-ip-runtime")]
    entries: PacketMetadataVec<Entry>,
    #[cfg(feature = "raw-ip-runtime")]
    source: std::sync::OnceLock<
        zero_platform_tokio::controlled_carrier::LogicalPacketProviderRegistration,
    >,
    #[cfg(feature = "raw-ip-runtime")]
    copies_installed: std::sync::OnceLock<()>,
    #[cfg(feature = "raw-ip-runtime")]
    taking: std::sync::atomic::AtomicBool,
}
#[cfg(feature = "raw-ip-runtime")]
struct Taking<'a>(&'a std::sync::atomic::AtomicBool);
#[cfg(feature = "raw-ip-runtime")]
impl Drop for Taking<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}
impl LogicalWorkspaces {
    pub(super) fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        identity: Option<(
            zero_core::component::ComponentInstanceId,
            zero_platform_tokio::contract::scope::TaskScopeIdentity,
        )>,
        tasks: Option<&zero_platform_tokio::contract::scope::WeakTaskRegistrar>,
        roots: &Arc<crate::HostOriginalBudgets>,
    ) -> Result<Self, ChannelError> {
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            let _ = (identity, tasks, roots);
            if declarations.iter().any(|declaration| {
                matches!(
                    declaration,
                    ResourceDeclaration::LogicalPacketWorkspaces { .. }
                )
            }) {
                return Err(error(
                    component,
                    Reason::Unsupported,
                    "software Packet memory is not compiled",
                ));
            }
            Ok(Self {})
        }
        #[cfg(feature = "raw-ip-runtime")]
        {
            let count = declarations
                .iter()
                .filter(|declaration| {
                    matches!(
                        declaration,
                        ResourceDeclaration::LogicalPacketWorkspaces { .. }
                    )
                })
                .count();
            let mut entries = PacketMetadataVec::default();
            let memory = roots.packet.driver_metadata_view();
            for (ordinal, declaration) in declarations.iter().enumerate() {
                let ResourceDeclaration::LogicalPacketWorkspaces {
                    workspaces,
                    control_workspace,
                    supplemental,
                } = declaration
                else {
                    continue;
                };
                let (instance, scope) = identity.as_ref().ok_or_else(|| {
                    error(
                        component,
                        Reason::InvalidConfig,
                        "software Packet memory lacks actual Host instance/Scope",
                    )
                })?;
                let tasks = tasks
                    .filter(|tasks| {
                        tasks.scope_identity().same_scope(scope)
                            && tasks.retirement_observation().is_some()
                    })
                    .ok_or_else(|| {
                        error(
                            component,
                            Reason::InvalidConfig,
                            "software Packet memory lacks its real cold registrar",
                        )
                    })?;
                let _ = tasks; // No registration is enabled by this memory-only prepayment.
                entries
                    .try_reserve_one_with_metadata(&memory, count)
                    .map_err(|source| {
                        error(
                            component,
                            Reason::Capacity,
                            "software Packet declaration table cannot fit",
                        )
                        .with_source(source)
                    })?;
                let original = HostPacketReadReservation::prepare_software(
                    *instance,
                    scope.clone(),
                    roots.clone(),
                    *workspaces,
                    *control_workspace,
                    supplemental,
                )?;
                entries
                    .push_reserved(Entry { ordinal, original })
                    .map_err(|_| {
                        error(
                            component,
                            Reason::Internal,
                            "prepaid software declaration row disappeared",
                        )
                    })?;
            }
            Ok(Self {
                component,
                entries,
                source: Default::default(),
                copies_installed: Default::default(),
                taking: Default::default(),
            })
        }
    }
    #[cfg(feature = "raw-ip-runtime")]
    fn enter(&self, component: ComponentId) -> Result<Taking<'_>, ChannelError> {
        self.taking
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map_err(|_| {
                error(
                    component,
                    Reason::Capacity,
                    "software memory admission is already active",
                )
            })?;
        Ok(Taking(&self.taking))
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) fn take(
        &self,
        ordinal: usize,
        driver: &TaskJoinWitness,
    ) -> Result<
        zero_platform_tokio::contract::component::GrantedLogicalPacketWorkspaces,
        ChannelError,
    > {
        let entry = self
            .entries
            .iter()
            .find(|entry| entry.ordinal == ordinal)
            .ok_or_else(|| {
                error(
                    ComponentId(0),
                    Reason::InvalidConfig,
                    "no software Packet workspace declaration",
                )
            })?;
        let _taking = self.enter(self.component)?;
        if self.entries.iter().any(|other| {
            other
                .original
                .software_driver()
                .is_some_and(|actual| actual.same_registration(driver))
        }) {
            return Err(error(
                self.component,
                Reason::StalePlan,
                "one actual wire cannot take another peer's workspace",
            ));
        }
        entry.original.take_software(driver)
    }
    pub(super) fn revoke(&self) {
        #[cfg(feature = "raw-ip-runtime")]
        for entry in &*self.entries {
            entry.original.revoke();
        }
    }
    pub(super) fn evidence(&self) -> Vec<read_workspaces::ReadEvidence> {
        #[cfg(feature = "raw-ip-runtime")]
        {
            self.entries
                .iter()
                .map(|entry| read_workspaces::ReadEvidence {
                    ordinal: entry.ordinal,
                    released: entry.original.software_released(),
                })
                .collect()
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            Vec::new()
        }
    }
}

#[cfg(all(test, feature = "raw-ip-runtime"))]
mod tests;
