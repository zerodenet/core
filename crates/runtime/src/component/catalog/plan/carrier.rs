//! Check borrowed lineage against actual installed roles and original providers.
use super::*;
mod metadata;
mod preparation;
mod process;
impl ComponentPlan {
    /// Planning capacity only; retain still validates each installed original
    /// through validate_carrier_adoption before it can enter the caller ledger.
    pub fn pending_carrier_capacity(
        &self,
        roles: &[zero_core::component::RoleInstanceId],
    ) -> Result<usize, ChannelError> {
        self.pending_carrier_capacity_union(roles, None)
    }
    /// Actual direct dependency, plus the selected prefix, in the same finite
    /// census. A provider reached twice is counted once, not added twice.
    pub fn pending_carrier_capacity_with_dependency(
        &self,
        consumer: zero_core::component::ComponentInstanceId,
        dependency: zero_core::component::ComponentInstanceId,
        roles: &[zero_core::component::RoleInstanceId],
    ) -> Result<usize, ChannelError> {
        let slot = self
            .component(consumer.component)
            .filter(|slot| slot.instance_id() == consumer)
            .filter(|slot| slot.dependency_instances().contains(&dependency))
            .ok_or_else(|| invalid(Reason::StalePlan, "pending direct dependency changed"))?;
        let _ = slot;
        self.pending_carrier_capacity_union(roles, Some(dependency))
    }
    fn pending_carrier_capacity_union(
        &self,
        roles: &[zero_core::component::RoleInstanceId],
        dependency: Option<zero_core::component::ComponentInstanceId>,
    ) -> Result<usize, ChannelError> {
        use zero_platform_tokio::contract::component::ResourceDeclaration as R;
        let mut pending = dependency.into_iter().collect::<Vec<_>>();
        for role in roles {
            pending.push(
                self.role_slot(*role)
                    .ok_or_else(|| invalid(Reason::StalePlan, "pending role changed"))?
                    .instance_id(),
            );
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut capacity = 0usize;
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            let slot = self
                .order
                .iter()
                .find(|slot| slot.instance_id() == id)
                .ok_or_else(|| invalid(Reason::StalePlan, "pending provider changed"))?;
            for resource in &slot.recipe.spec().resources {
                let limit = match resource {
                    R::ControlledServiceStream { max_sockets, .. }
                    | R::ControlledServiceDatagram { max_sockets, .. }
                    | R::ControlledCarrierPool { max_sockets, .. }
                    | R::ControlledAssociationPool { max_sockets, .. }
                    | R::ControlledNodeStream { max_sockets, .. }
                    | R::ControlledNodeCarrier { max_sockets, .. }
                    | R::ControlledDirectStream { max_sockets, .. }
                    | R::ControlledOutboundStream { max_sockets, .. }
                    | R::ControlledDatagramCarrier { max_sockets, .. } => max_sockets.get(),
                    _ => 0,
                };
                capacity = capacity.checked_add(limit).ok_or_else(|| {
                    invalid(Reason::Capacity, "pending original capacity overflow")
                })?;
            }
            pending.extend(self.retirement_dependencies(slot));
        }
        Ok(capacity)
    }

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
                    zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool { max_sockets, .. } | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledAssociationPool { max_sockets, .. } => {
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

    /// Exact installed native-metadata/borrowed-datagram pair. This only
    /// selects an executable provider; grants still verify every original.
    pub fn has_metadata_stream_provider(&self, role: zero_core::component::RoleInstanceId) -> bool {
        self.role_slot(role)
            .is_some_and(|slot| metadata::installed(slot))
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
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeCarrier {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledServiceStream {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledServiceDatagram {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledAssociationPool { .. } | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledMetadataCarrier {..})) {return true;}
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
            // This is only the exact declared same-job pair. Actual endpoint
            // demand still checks the direct dependency, original grant and
            // both started slots through ensure_process_endpoint_use.
            if process::paired_endpoint(slot.recipe.spec()) || metadata::installed(slot) {
                pending.extend(slot.dependency_instances());
                continue;
            }
            if slot.recipe.spec().resources.iter().any(|resource|!matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::File {..} |
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeCarrier {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledServiceStream {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledServiceDatagram {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool {..} | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledAssociationPool { .. } |
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
        self.validate_carrier_instance(selected.instance_id(), adoption)
    }
    pub(crate) fn validate_carrier_instance(
        &self,
        consumer: zero_core::component::ComponentInstanceId,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let fail = || {
            invalid(
                Reason::StalePlan,
                "pending carrier differs from the actual owning graph",
            )
        };
        if self
            .component(consumer.component)
            .is_none_or(|slot| slot.instance_id() != consumer)
        {
            return Err(fail());
        }
        let mut pending = vec![consumer];
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
            if !origin.is_retired() && slot.matches_packet_source(origin) {
                return Ok(());
            }
            if id == origin.instance() && !origin.is_retired() {
                let matches = slot.recipe.spec().resources.iter().enumerate().any(
                    |(ordinal, _)| match origin.owner() {
                        // A software original can match only the exact source
                        // installed above, never a declared native provider.
                        zero_platform_tokio::controlled_carrier::CarrierOriginOwner::LogicalPacket(_) => false,
                        zero_platform_tokio::controlled_carrier::CarrierOriginOwner::Packet(
                            actual,
                        ) => slot
                            .original_packet_owner(ordinal)
                            .is_some_and(|owner| owner.same_owner(actual)),
                        zero_platform_tokio::controlled_carrier::CarrierOriginOwner::Listener(
                            _,
                        ) => slot.matches_bound_packet_origin(ordinal, origin),
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
            ResourceDeclaration::ControlledServiceStream {
                endpoint,
                dial,
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
