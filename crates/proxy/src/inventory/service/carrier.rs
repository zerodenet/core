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
                    let borrowed = spec.task_limit.is_none()
                        && components.has_carrier_provider(slot.role_id())
                        && components.stream_dependencies_supported(slot.role_id());
                    // A single taskless literal root already carries this exact
                    // policy in its original-provider declaration. This is only
                    // selection: the actual grant still checks endpoint/dial.
                    let custom_root = !relay
                        && borrowed
                        && spec.dependencies.is_empty()
                        && spec.resources.iter().all(|resource| {
                            matches!(
                                resource,
                                ResourceDeclaration::ControlledCarrierPool {
                                    datagram: false,
                                    ..
                                } | ResourceDeclaration::File { .. }
                            )
                        })
                        && spec.resources.iter().any(|resource| {
                            matches!(resource,
                                ResourceDeclaration::ControlledCarrierPool {
                                    endpoint, dial, datagram: false, ..
                                } if dial == &slot.dial
                                    && matches!(&endpoint.address,
                                        zero_core::Address::Ipv4(_) | zero_core::Address::Ipv6(_)))
                        });
                    if slot.dial != Default::default() && !(direct && !relay || custom_root)
                        || !spec.roles.contains(&RoleKind::Stream)
                        || direct && relay
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
