//! Actual finite per-connection preparation provider, never cached custody.
use super::*;
impl ComponentPlan {
    /// The existing one-ordinal per-connection owner owns its real setup Scope.
    /// This is not cached-driver custody and cannot transfer a cleanup claim.
    pub fn has_preparing_node_carrier_provider(
        &self,
        role: zero_core::component::RoleInstanceId,
    ) -> bool {
        use zero_platform_tokio::contract::component::{ResourceDeclaration as R, RoleKind};
        let Some(slot) = self.role_slot(role) else {
            return false;
        };
        let spec = slot.recipe.spec();
        let Some(limit) = spec.task_limit else {
            return false;
        };
        if !spec.roles.contains(&RoleKind::Stream) {
            return false;
        }
        let mut node = None;
        let mut packets = Vec::new();
        for (ordinal, resource) in spec.resources.iter().enumerate() {
            match resource {
                R::ControlledNodeCarrier {
                    endpoint,
                    dial,
                    max_sockets,
                } if *max_sockets == limit && node.is_none() => {
                    node = Some((ordinal, endpoint, dial));
                }
                R::ControlledCarrierPool {
                    endpoint,
                    dial,
                    datagram: true,
                    max_sockets,
                } => {
                    packets.push((ordinal, endpoint, dial, max_sockets));
                }
                R::File { .. } => {}
                _ => return false,
            }
        }
        let Some((ordinal, endpoint, dial)) = node else {
            return false;
        };
        let Some(owner) = slot.original_carrier_owner(ordinal) else {
            return false;
        };
        if owner.instance() != slot.instance_id() || owner.limit() != limit {
            return false;
        }
        let Some(bound) = owner.bound() else {
            return false;
        };
        // The existing SS aggregate Datagram role remains its own real
        // declaration/owner. It cannot be inferred from a resource marker.
        packets
            .into_iter()
            .all(|(ordinal, actual_endpoint, actual_dial, capacity)| {
                actual_endpoint == endpoint
                    && actual_dial == dial
                    && slot.original_carrier_owner(ordinal).is_some_and(|packet| {
                        packet.instance() == slot.instance_id()
                            && packet.limit() == *capacity
                            && packet.bound().is_some_and(|actual| {
                                actual.scope_identity().same_scope(&bound.scope_identity())
                            })
                    })
            })
    }
}
