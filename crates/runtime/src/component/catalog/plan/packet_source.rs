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
    /// A Packet-only native owner is sufficient for L3 dispatch; Stream and
    /// Datagram qualification remains a separate three-role source check.
    pub fn has_packet_provider(&self, role: zero_core::component::RoleInstanceId) -> bool {
        let Some(selected) = self.role_slot(role) else {
            return false;
        };
        if !selected
            .recipe
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Packet)
        {
            return false;
        }
        let mut pending = vec![selected.instance_id()];
        let mut seen = std::collections::BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let Some(slot) = self.order.iter().find(|slot| slot.instance_id() == id) else {
                return false;
            };
            if slot.packet_provider_ready() {
                return true;
            }
            pending.extend(slot.dependency_instances());
        }
        false
    }
    pub fn has_device_carrier_provider(&self, role: zero_core::component::RoleInstanceId) -> bool {
        let Some(selected) = self.role_slot(role) else {
            return false;
        };
        let spec = selected.recipe.spec();
        if ![
            zero_platform_tokio::contract::component::RoleKind::Packet,
            zero_platform_tokio::contract::component::RoleKind::Stream,
            zero_platform_tokio::contract::component::RoleKind::Datagram,
        ]
        .iter()
        .all(|role| spec.roles.contains(role))
        {
            return false;
        }
        let mut pending = vec![selected.instance_id()];
        let mut seen = std::collections::BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let Some(slot) = self.order.iter().find(|slot| slot.instance_id() == id) else {
                return false;
            };
            if slot.device_carrier_ready() {
                return true;
            }
            pending.extend(slot.dependency_instances());
        }
        false
    }
}
