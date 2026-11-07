//! Inspect executable standard roles without consuming a scheduler pick.
use super::*;
use zero_engine::{EngineRuntimeSnapshot, TargetId, TargetKind};
use zero_platform_tokio::contract::component::{ResourceDeclaration, RoleKind};
impl ProtocolInventory {
    pub(crate) fn carrier_service_target(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        target: TargetId,
        components: &ComponentPlan,
    ) -> bool {
        let mut pending = vec![(target, false)];
        let mut seen = std::collections::BTreeSet::new();
        let mut leaves = 0;
        while let Some((id, relay)) = pending.pop() {
            if !seen.insert((id, relay)) {
                continue;
            }
            let Some(node) = snapshot.plan().target(id) else {
                return false;
            };
            match node.kind() {
                TargetKind::Outbound(outbound) => {
                    let Some(slot) = components.configured(node.tag()) else {
                        return false;
                    };
                    let spec = slot.recipe.spec();
                    let direct =
                        outbound.runtime_kind() == zero_config::OutboundRuntimeKind::Direct;
                    // Shared roles borrow their donor's actual logical control.
                    // The cold graph must contain an integrated carrier provider;
                    // taskless framing alone still uses the caller's DNS original.
                    let borrowed = components.has_device_carrier_provider(slot.role_id())
                        || (spec.task_limit.is_none()
                            || components.has_preparing_node_carrier_provider(slot.role_id())
                            || components.has_node_stream_provider(slot.role_id()))
                            && components.has_carrier_provider(slot.role_id())
                            && components.stream_dependencies_supported(slot.role_id());
                    if slot.dial != Default::default() && !direct
                        || !spec.roles.contains(&RoleKind::Stream)
                        || !direct
                            && !borrowed
                            && (spec.task_limit.is_some()
                                || !spec.dependencies.is_empty()
                                || !spec.resources.iter().all(|resource| {
                                    matches!(
                                        resource,
                                        ResourceDeclaration::ControlledCarrierPool {
                                            datagram: false,
                                            ..
                                        } | ResourceDeclaration::File { .. }
                                    )
                                }))
                    {
                        return false;
                    }
                    leaves += 1;
                }
                TargetKind::Selector(group) => {
                    pending.extend(group.members().iter().map(|id| (*id, relay)))
                }
                TargetKind::UrlTest(group) => {
                    pending.extend(group.members().iter().map(|id| (*id, relay)))
                }
                TargetKind::LoadBalance(group) => {
                    pending.extend(group.members().iter().map(|id| (*id, relay)))
                }
                TargetKind::Relay(group) => {
                    pending.extend(group.chain().iter().map(|id| (*id, true)))
                }
                TargetKind::Fallback(group) => {
                    pending.extend(group.members().iter().map(|id| (*id, relay)))
                }
            }
        }
        leaves != 0
    }
}
