//! One actual wire's prepaid working copy, in the existing Kernel TX lane.
use super::*;
mod send;
pub use send::{PacketDriverSendBuffers, PacketDriverSendCopy};
#[cfg(feature = "native-packet-origin")]
#[derive(Clone)]
pub(super) struct WireCopy {
    pub(super) driver: TaskJoinWitness,
    pub(super) grant: zero_tun::packet_memory::NativeReadGrant,
    target: zero_tun::packet_memory::NativeReadGrant,
}
#[cfg(feature = "native-packet-origin")]
impl PacketDriverCalls {
    pub(super) fn metadata_memory(
        &self,
    ) -> Result<zero_tun::packet_memory::PacketMemoryView, ChannelError> {
        self.copy_workspaces
            .get()
            .and_then(|copies| copies.first())
            .ok_or_else(|| failure(Reason::Revoked))?
            .grant
            .memory_view()
            .map_err(memory_error)
    }

    /// Only the concrete Host/Source owner holds this Calls value. It passes
    /// the same pre-native grants from its declared original memory holder.
    /// Consumers receive no Root/policy setter and reserve no second pool.
    pub fn install_send_workspaces(
        &self,
        original: &CarrierOrigin,
        grants: Vec<(
            TaskJoinWitness,
            zero_tun::packet_memory::NativeReadGrant,
            zero_tun::packet_memory::NativeReadGrant,
        )>,
    ) -> Result<(), ChannelError> {
        let actual: Vec<_> = if let Some(logical) = self.original.logical_packet() {
            logical.drivers().to_vec()
        } else {
            self.original.driver().cloned().into_iter().collect()
        };
        let root = grants
            .first()
            .ok_or_else(|| failure(Reason::StalePlan))?
            .1
            .memory_view()
            .map_err(memory_error)?;
        if !self.original.same_original(original)
            || grants.len() != actual.len()
            || grants.is_empty()
            || self.copy_workspaces.get().is_some()
            || grants
                .iter()
                .enumerate()
                .any(|(index, (driver, grant, target))| {
                    driver.is_joined()
                        || !actual.iter().any(|wire| wire.same_registration(driver))
                        || grants[..index]
                            .iter()
                            .any(|(previous, _, _)| previous.same_registration(driver))
                        || grant.bytes() == 0
                        || !grant
                            .memory_view()
                            .is_ok_and(|view| view.same_kernel_root(&root))
                        || target.bytes() < 255
                        || !target
                            .memory_view()
                            .is_ok_and(|view| view.same_kernel_root(&root))
                })
        {
            return Err(failure(Reason::StalePlan));
        }
        let grants = grants
            .into_iter()
            .map(|(driver, grant, target)| WireCopy {
                driver,
                grant,
                target,
            })
            .collect::<Vec<_>>();
        self.copy_workspaces
            .set(grants.into())
            .map_err(|_| failure(Reason::StalePlan))
    }
}
impl PacketDriverDatagramHandoff {
    #[cfg(feature = "native-packet-origin")]
    pub(super) fn install_receiver_copy(
        &self,
        actual: &TaskJoinWitness,
    ) -> Result<(), ChannelError> {
        let copy = self
            .record
            .copies
            .iter()
            .find(|copy| copy.driver.same_registration(actual))
            .ok_or_else(|| failure(Reason::StalePlan))?;
        if let Some(installed) = self.installed.memory.get() {
            return (installed.driver.same_registration(actual)
                && installed
                    .grant
                    .memory_view()
                    .ok()
                    .zip(copy.grant.memory_view().ok())
                    .is_some_and(|(left, right)| left.same_original(&right))
                && installed
                    .target
                    .memory_view()
                    .ok()
                    .zip(copy.target.memory_view().ok())
                    .is_some_and(|(left, right)| left.same_original(&right)))
            .then_some(())
            .ok_or_else(|| failure(Reason::StalePlan));
        }
        self.installed
            .memory
            .set(copy.clone())
            .map_err(|_| failure(Reason::StalePlan))
    }
}
#[cfg(feature = "native-packet-origin")]
pub(super) fn memory_error(source: zero_tun::packet_memory::PacketMemoryError) -> ChannelError {
    use zero_tun::packet_memory::PacketMemoryError;
    let reason = match source {
        PacketMemoryError::Stopped => Reason::Revoked,
        PacketMemoryError::Capacity | PacketMemoryError::Busy | PacketMemoryError::Paused => {
            Reason::Capacity
        }
        PacketMemoryError::Overflow => Reason::InvalidConfig,
    };
    failure(reason).with_source(source)
}

#[cfg(feature = "native-packet-origin")]
impl super::GrantedPacketServiceCaller {
    /// Observation only. This does not create/reserve another workspace or
    /// issue IO; actual Source/Host declarations supplied these original slots.
    pub fn matches_memory_root(&self, memory: &zero_tun::packet_memory::PacketMemoryView) -> bool {
        self.packet_calls()
            .copy_workspaces
            .get()
            .is_some_and(|copies| {
                copies.iter().any(|copy| {
                    copy.grant
                        .memory_view()
                        .is_ok_and(|actual| actual.same_kernel_root(memory))
                })
            })
    }
}
