//! Finite installed TCP or metadata+datagram half issuers, no IO permission.
use super::*;
use zero_platform_tokio::contract::component::{ResourceDeclaration as R, RoleKind};
pub(super) fn installed(slot: &Arc<ComponentSlot>) -> bool {
    let spec = slot.recipe.spec();
    if !spec.roles.contains(&RoleKind::Stream) {
        return false;
    }
    let (mut capacity, mut halves, mut metadata, mut minimum) = (None, 0usize, 0usize, 0usize);
    for (ordinal, resource) in spec.resources.iter().enumerate() {
        let (limit, tasks) = match resource {
            R::ControlledMetadataCarrier {
                endpoint,
                dial,
                max_origins,
                ..
            } => {
                let Some(R::ControlledCarrierPool {
                    endpoint: other,
                    dial: policy,
                    max_sockets,
                    datagram: true,
                }) = spec.resources.get(ordinal + 1)
                else {
                    return false;
                };
                if endpoint != other || dial != policy || max_origins != max_sockets {
                    return false;
                }
                if !slot.original_metadata_owner(ordinal).is_some_and(|owner| {
                    owner.instance() == slot.instance_id() && owner.limit() == *max_origins
                }) {
                    return false;
                }
                halves += 1;
                metadata += 1;
                (*max_origins, 4usize)
            }
            R::ControlledCarrierPool {
                endpoint,
                dial,
                max_sockets,
                datagram: true,
            } => {
                let Some(R::ControlledMetadataCarrier {
                    endpoint: other,
                    dial: policy,
                    max_origins,
                    ..
                }) = ordinal
                    .checked_sub(1)
                    .and_then(|index| spec.resources.get(index))
                else {
                    return false;
                };
                if endpoint != other
                    || dial != policy
                    || max_sockets != max_origins
                    || !carrier_installed(slot, ordinal, *max_sockets)
                {
                    return false;
                }
                (*max_sockets, 4usize)
            }
            R::ControlledCarrierPool {
                max_sockets,
                datagram: false,
                ..
            } => {
                if !carrier_installed(slot, ordinal, *max_sockets) {
                    return false;
                }
                halves += 1;
                (*max_sockets, 2usize)
            }
            R::File { .. } => continue,
            R::NativeIcmp { max_sockets } => (*max_sockets, 0usize),
            _ => return false,
        };
        if capacity.is_some_and(|actual| actual != limit) {
            return false;
        }
        capacity = Some(limit);
        minimum = minimum.max(match limit.get().checked_mul(tasks) {
            Some(actual) => actual,
            None => return false,
        });
    }
    let icmp = spec
        .resources
        .iter()
        .any(|resource| matches!(resource, R::NativeIcmp { .. }));
    (1..=2).contains(&halves)
        && (!icmp || metadata > 0)
        && spec
            .task_limit
            .is_some_and(|actual| actual.get() >= minimum)
}
fn carrier_installed(
    slot: &Arc<ComponentSlot>,
    ordinal: usize,
    limit: std::num::NonZeroUsize,
) -> bool {
    slot.original_carrier_owner(ordinal).is_some_and(|owner| {
        owner.instance() == slot.instance_id()
            && owner.limit() == limit
            && owner.bound().is_some_and(|bound| {
                slot.matches_live_scope(&bound.scope_identity())
                    && bound.matches_registration(&owner)
            })
    })
}
