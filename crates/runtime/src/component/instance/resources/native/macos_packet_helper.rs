//! Concrete same-provider metadata/actual-helper receipt access, never IO grant.
use super::*;
use zero_platform_tokio::contract::{component::PacketDeviceIdentity, scope::TaskJoinWitness};
impl HostResources {
    pub fn prepare_macos_packet_helper(
        &self,
        name: &str,
        output_limit: usize,
        request_capacity: usize,
    ) -> Result<
        (
            PacketDeviceIdentity,
            zero_tun::MacosHelperAccess,
            Option<TaskJoinWitness>,
        ),
        ChannelError,
    > {
        if self.cancel.is_cancelled()
            || self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed
        {
            return Err(self.denied());
        }
        let tasks = self.original_tasks.as_ref().ok_or_else(|| self.denied())?;
        let scope = tasks.scope_identity();
        if !scope.registration_open() {
            return Err(self.denied());
        }
        let instance = self.instance.ok_or_else(|| self.denied())?;
        let owner = self
            .packets
            .iter()
            .find(|owner| owner.name() == name && owner.source_matches(instance, &scope))
            .ok_or_else(|| self.denied())?;
        owner.prepare_macos_helper(output_limit, request_capacity)
    }
    pub fn stop_macos_packet_helpers(&self) {
        for owner in &self.packets {
            owner.stop_macos_helper();
        }
    }
}
