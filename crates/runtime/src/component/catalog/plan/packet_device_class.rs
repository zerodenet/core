//! Read-only actual declaration/dependency class, never IO or closure authority.
use super::*;
impl ComponentPlan {
    pub fn has_requested_ring_dependencies(
        &self,
        dependencies: &[zero_core::component::ComponentInstanceId],
    ) -> Result<bool, ChannelError> {
        for instance in dependencies {
            if self.requested_ring_dependency(*instance, self.order.len())? {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn requested_ring_dependency(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        left: usize,
    ) -> Result<bool, ChannelError> {
        if left == 0 {
            return Err(invalid(
                Reason::StalePlan,
                "actual packet-device dependency graph is cyclic or absent",
            ));
        }
        let slot = self
            .component(instance.component)
            .filter(|slot| slot.instance_id() == instance)
            .ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "actual source packet-device dependency generation is unavailable",
                )
            })?;
        if slot.recipe.spec().resources.iter().any(|resource| {
            cfg!(windows) && matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledPacketDevice {
                    binding: zero_platform_tokio::contract::component::PacketDeviceBinding::Create {..}
                        | zero_platform_tokio::contract::component::PacketDeviceBinding::ExistingWindows {..}, ..})
        }) { return Ok(true); }
        for dependency in slot.dependency_instances() {
            if self.requested_ring_dependency(*dependency, left - 1)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
