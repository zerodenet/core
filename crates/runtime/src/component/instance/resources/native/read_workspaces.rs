//! Concrete read allocations for one original declared UDP descriptor.
mod construction;
mod declarations;
pub use construction::{
    packet_datagram_control_spec, packet_datagram_read_spec, packet_datagram_supplemental_spec,
};
#[cfg(feature = "raw-ip-runtime")]
mod supplemental;
use super::*;
pub(super) use declarations::ReadDeclarations;
pub(in crate::component) use declarations::ReadEvidence;
use std::num::NonZeroUsize;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity};
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
    #[cfg(feature = "raw-ip-runtime")]
    supplemental: zero_platform_tokio::contract::component::PacketSupplementalMemory,
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
    #[cfg(feature = "raw-ip-runtime")]
    supplemental: Option<supplemental::OriginalSupplemental>,
    #[cfg(feature = "raw-ip-runtime")]
    received: supplemental::ReceivedFlags,
    driver: Option<TaskJoinWitness>,
}
impl HostPacketReadReservation {
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
        #[cfg(feature = "raw-ip-runtime")]
        let _notifications = self.roots.packet.defer_notifications();
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
                let (grants, supplemental, received) = self.allocate_original_workspaces()?;
                state.received = received;
                state.supplemental = Some(supplemental);
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
        let grants = state.grants.take();
        #[cfg(feature = "raw-ip-runtime")]
        let supplemental = state.supplemental.take();
        #[cfg(feature = "raw-ip-runtime")]
        let received = std::mem::take(&mut state.received);
        drop(state);
        #[cfg(feature = "raw-ip-runtime")]
        drop(received);
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(supplemental) = supplemental {
            supplemental.revoke();
        }
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
