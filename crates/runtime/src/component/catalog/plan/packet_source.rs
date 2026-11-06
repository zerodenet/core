//! Actual source projection, never a declaration-only provider grant.
use super::*;
impl ComponentPlan {
    /// Called only by host assembly with the exact candidate component and the
    /// source's actual acquired native/wire registration. No native authority
    /// or cleanup responsibility moves out of that source's original owner.
    pub fn bind_packet_source_provider(
        &self,
        candidate: zero_core::component::ComponentInstanceId,
        actual: zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
    ) -> Result<(), ChannelError> {
        self.order
            .iter()
            .find(|slot| slot.instance_id() == candidate)
            .ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "Packet source target is outside this actual candidate",
                )
            })?
            .bind_packet_source(actual)
    }
    pub fn has_device_carrier_provider(&self, role: zero_core::component::RoleInstanceId) -> bool {
        self.role_slot(role)
            .is_some_and(|slot| slot.device_carrier_ready())
    }
}
