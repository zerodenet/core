//! Concrete read allocations for one original declared UDP descriptor.
mod declarations;
use super::*;
pub(super) use declarations::ReadDeclarations;
pub(in crate::component) use declarations::ReadEvidence;
use std::num::NonZeroUsize;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity};
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
/// Host construction data only. A protocol receives the unique original grants
/// through ResourceAccess, never this accounting root or a new provider.
pub struct HostPacketReadReservation {
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    #[cfg(feature = "raw-ip-runtime")]
    lengths: [NonZeroUsize; 2],
    #[cfg(feature = "raw-ip-runtime")]
    control: Option<NonZeroUsize>,
    #[cfg(feature = "raw-ip-runtime")]
    roots: Arc<crate::HostOriginalBudgets>,
    state: Mutex<ReadState>,
}
#[derive(Default)]
struct ReadState {
    stopped: bool,
    taken: bool,
    #[cfg(feature = "raw-ip-runtime")]
    control_taken: bool,
    #[cfg(feature = "raw-ip-runtime")]
    grants: Option<zero_tun::packet_memory::PacketDatagramWorkspaces>,
    driver: Option<TaskJoinWitness>,
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
        if !cfg!(feature = "raw-ip-runtime") {
            return Err(error(
                instance.component,
                Reason::Unsupported,
                "actual packet read memory unavailable",
            ));
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = (roots, lengths, control);
        Ok(Arc::new(Self {
            instance,
            scope,
            #[cfg(feature = "raw-ip-runtime")]
            roots,
            #[cfg(feature = "raw-ip-runtime")]
            lengths,
            #[cfg(feature = "raw-ip-runtime")]
            control,
            state: Mutex::new(ReadState::default()),
        }))
    }
    pub fn prepare_before_listener_bind(&self, tasks: &TaskRegistrar) -> Result<(), ChannelError> {
        if !self.scope.matches(tasks) {
            return Err(self.denied());
        }
        self.prepare(None)
    }
    fn prepare_for_weak_scope(
        &self,
        tasks: &zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    ) -> Result<(), ChannelError> {
        if !tasks.scope_identity().same_scope(&self.scope) || !tasks.registration_open() {
            return Err(self.denied());
        }
        self.prepare(None)
    }
    pub fn prepare_for_driver(&self, driver: &TaskJoinWitness) -> Result<(), ChannelError> {
        if !driver.scope_identity().same_scope(&self.scope) || driver.is_joined() {
            return Err(self.denied());
        }
        self.prepare(Some(driver))
    }
    fn prepare(&self, driver: Option<&TaskJoinWitness>) -> Result<(), ChannelError> {
        if !self.scope.resource_open() {
            return Err(self.denied());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped
            || state
                .driver
                .as_ref()
                .is_some_and(|actual| driver.is_none_or(|next| !actual.same_registration(next)))
        {
            return Err(self.denied());
        }
        #[cfg(feature = "raw-ip-runtime")]
        {
            if state.grants.is_none() {
                let grants = self
                    .roots
                    .packet
                    .datagram_workspaces(
                        self.lengths.map(NonZeroUsize::get),
                        self.control.map(NonZeroUsize::get),
                    )
                    .map_err(|source| {
                        error(
                            self.instance.component,
                            Reason::Capacity,
                            "original UDP read workspaces cannot fit before native acquisition",
                        )
                        .with_source(source)
                    })?;
                state.grants = Some(grants);
            }
        }
        if let Some(driver) = driver {
            state.driver.get_or_insert_with(|| driver.clone());
        }
        Ok(())
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub fn take(&self) -> Result<[zero_tun::packet_memory::NativeReadGrant; 2], ChannelError> {
        if !self.scope.resource_open() {
            return Err(self.denied());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || state.taken {
            return Err(self.denied());
        }
        let grants = state
            .grants
            .as_ref()
            .ok_or_else(|| self.denied())?
            .reads
            .clone();
        state.taken = true;
        Ok(grants)
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub fn take_control(
        &self,
    ) -> Result<Option<zero_tun::packet_memory::NativeReadGrant>, ChannelError> {
        if !self.scope.resource_open() {
            return Err(self.denied());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || !state.taken || state.control_taken {
            return Err(self.denied());
        }
        let control = state
            .grants
            .as_ref()
            .ok_or_else(|| self.denied())?
            .control
            .clone();
        state.control_taken = true;
        Ok(control)
    }
    /// Read-only view of this exact retained read0; never retakes unique IO.
    #[cfg(feature = "raw-ip-runtime")]
    pub fn packet_memory_view(
        &self,
    ) -> Result<zero_tun::packet_memory::PacketMemoryView, ChannelError> {
        if !self.scope.resource_open() {
            return Err(self.denied());
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || !state.taken {
            return Err(self.denied());
        }
        state.grants.as_ref().ok_or_else(|| self.denied())?.reads[0]
            .memory_view()
            .map_err(|source| self.denied().with_source(source))
    }
    pub fn revoke(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.stopped = true;
        #[cfg(feature = "raw-ip-runtime")]
        let grants = state.grants.clone();
        drop(state);
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(grants) = grants {
            for grant in grants {
                grant.revoke();
            }
        }
    }
    /// Admission ended, or no read memory was acquired. Held payload allocations
    /// remain charged to the same root; this is no native/driver close receipt.
    fn released(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(feature = "raw-ip-runtime")]
        {
            state.stopped || state.grants.is_none()
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        state.stopped
    }
    fn denied(&self) -> ChannelError {
        error(
            self.instance.component,
            Reason::Revoked,
            "original UDP read workspace is unavailable",
        )
    }
}

#[cfg(test)]
mod tests;
