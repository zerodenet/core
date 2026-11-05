//! Check borrowed lineage against actual installed roles and original providers.
use super::*;
impl ComponentPlan {
    /// Actual installed dynamic background stream/packet pools. Metadata QUIC roots do
    /// not acquire a new nonempty-prefix capability through this selector.
    pub fn has_background_carrier_pool(&self, role: zero_core::component::RoleInstanceId) -> bool {
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
            if slot.recipe.spec().resources.iter().enumerate().any(|(ordinal, resource)| {
                match resource {
                    zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool { max_sockets, .. } => {
                        slot.original_carrier_owner(ordinal).is_some_and(|owner| {
                            owner.instance() == slot.instance_id() && owner.limit() == *max_sockets
                        })
                    }
                    _ => false,
                }
            }) { return true; }
            pending.extend(slot.dependency_instances());
        }
        false
    }

    /// This exact ordinary owner has both typed issuers installed by its same
    /// host Slot. Declarations alone do not qualify a resourceful Stream loan.
    pub fn has_node_stream_provider(&self, role: zero_core::component::RoleInstanceId) -> bool {
        let Some(slot) = self.role_slot(role) else {
            return false;
        };
        let Some((node, service, capacity)) = node_stream_declarations(slot.recipe.spec()) else {
            return false;
        };
        [node, service].into_iter().all(|ordinal| {
            slot.original_carrier_owner(ordinal).is_some_and(|owner| {
                owner.instance() == slot.instance_id() && owner.limit() == capacity
            })
        })
    }

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
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledMetadataCarrier {..})) {return true;}
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
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool {..} |
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledMetadataCarrier {..} |
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledMetadataDatagram {..})
                && !(matches!(resource, zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeStream { .. })
                    && node_stream_declarations(slot.recipe.spec()).is_some())) {return false;}
            pending.extend(slot.dependency_instances());
        }
        true
    }
    /// Validate an actual pending original against the selected role's same
    /// installed resource graph before any native acquisition or borrowing.
    pub fn validate_carrier_adoption(
        &self,
        role: zero_core::component::RoleInstanceId,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let fail = || {
            invalid(
                Reason::StalePlan,
                "pending carrier is not owned by the actual selected role graph",
            )
        };
        let selected = self.role_slot(role).ok_or_else(fail)?;
        let mut pending = vec![selected.instance_id()];
        let mut seen = std::collections::BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let slot = self
                .order
                .iter()
                .find(|slot| slot.instance_id() == id)
                .ok_or_else(fail)?;
            if id == adoption.instance()
                && slot
                    .recipe
                    .spec()
                    .resources
                    .iter()
                    .enumerate()
                    .any(|(ordinal, _)| {
                        slot.original_carrier_owner(ordinal)
                            .is_some_and(|owner| owner.matches_adoption(adoption))
                    })
            {
                return Ok(());
            }
            pending.extend(self.retirement_dependencies(slot));
        }
        Err(fail())
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
                let matches = slot.recipe.spec().resources.iter().enumerate().any(
                    |(ordinal, _)| match origin.owner() {
                        zero_platform_tokio::controlled_carrier::CarrierOriginOwner::Carrier(
                            actual,
                        ) => slot
                            .original_carrier_owner(ordinal)
                            .is_some_and(|owner| owner.same_owner(actual)),
                        zero_platform_tokio::controlled_carrier::CarrierOriginOwner::Metadata(
                            actual,
                        ) => slot
                            .original_metadata_owner(ordinal)
                            .is_some_and(|owner| owner.same_owner(actual)),
                    },
                );
                if matches {
                    return Ok(());
                }
            }
            pending.extend(self.retirement_dependencies(slot));
        }
        Err(fail())
    }
}

fn node_stream_declarations(
    spec: &zero_platform_tokio::contract::component::ComponentSpec,
) -> Option<(usize, usize, std::num::NonZeroUsize)> {
    use zero_platform_tokio::contract::component::{ResourceDeclaration, RoleKind};
    let capacity = spec.task_limit?;
    if !spec.roles.contains(&RoleKind::Stream) || !spec.roles.contains(&RoleKind::Datagram) {
        return None;
    }
    let mut node = None;
    let mut service = None;
    for (ordinal, resource) in spec.resources.iter().enumerate() {
        match resource {
            ResourceDeclaration::ControlledNodeStream {
                endpoint,
                dial,
                max_sockets,
            } if *max_sockets == capacity && node.is_none() => {
                node = Some((ordinal, endpoint, dial));
            }
            ResourceDeclaration::ControlledCarrierPool {
                endpoint,
                dial,
                datagram: false,
                max_sockets,
            } if *max_sockets == capacity && service.is_none() => {
                service = Some((ordinal, endpoint, dial));
            }
            ResourceDeclaration::File { .. } => {}
            _ => return None,
        }
    }
    let (node_ordinal, endpoint, dial) = node?;
    let (service_ordinal, service_endpoint, service_dial) = service?;
    (endpoint == service_endpoint && dial == service_dial).then_some((
        node_ordinal,
        service_ordinal,
        capacity,
    ))
}
