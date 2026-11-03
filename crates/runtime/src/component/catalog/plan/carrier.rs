//! Check borrowed lineage against actual installed roles and original providers.
use super::*;
impl ComponentPlan {
    /// Planning prerequisite only; actual borrowing still validates the original
    /// registration and executable role before using its transferred stream.
    pub fn has_carrier_provider(&self, role: zero_core::component::RoleInstanceId) -> bool {
        let Some(selected) = self.role_slot(role) else {
            return false;
        };
        let mut pending = vec![selected.instance_id()];
        let mut seen = std::collections::BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let Some(slot) = self.order.iter().find(|slot| slot.instance_id() == id) else {
                return false;
            };
            if slot.recipe.spec().resources.iter().any(|resource|matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool {..})) {return true;}
            pending.extend(slot.dependency_instances());
        }
        false
    }
    /// Non-I/O shared state (for example replay maintenance) is allowed inline.
    /// A process/device/packet owner cannot be mistaken for a borrowed TCP pool.
    pub fn stream_dependencies_supported(
        &self,
        role: zero_core::component::RoleInstanceId,
    ) -> bool {
        let Some(selected) = self.role_slot(role) else {
            return false;
        };
        let mut pending = vec![selected.instance_id()];
        let mut seen = std::collections::BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let Some(slot) = self.order.iter().find(|slot| slot.instance_id() == id) else {
                return false;
            };
            if slot.recipe.spec().resources.iter().any(|resource|!matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::File {..} |
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool {..})) {return false;}
            pending.extend(slot.dependency_instances());
        }
        true
    }
    pub fn validate_carrier_origin(
        &self,
        role: zero_core::component::RoleInstanceId,
        origin: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> Result<(), ChannelError> {
        let fail = || {
            invalid(
                Reason::StalePlan,
                "borrowed carrier does not belong to the actual selected role graph",
            )
        };
        let selected = self.role_slot(role).ok_or_else(fail)?;
        let mut pending = vec![selected.instance_id()];
        let mut visited = std::collections::BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !visited.insert(id) {
                continue;
            }
            let slot = self
                .order
                .iter()
                .find(|slot| slot.instance_id() == id)
                .ok_or_else(fail)?;
            if id == origin.instance() && !origin.is_retired() {
                let matches =
                    slot.recipe
                        .spec()
                        .resources
                        .iter()
                        .enumerate()
                        .any(|(ordinal, _)| {
                            slot.original_carrier_owner(ordinal)
                                .is_some_and(|owner| owner.same_owner(origin.owner()))
                        });
                if matches {
                    return Ok(());
                }
            }
            pending.extend(self.retirement_dependencies(slot));
        }
        Err(fail())
    }
}
