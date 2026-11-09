//! Data-only shape plus the actual two Host-installed issuers, never IO authority.
use super::*;
use zero_platform_tokio::contract::component::{ResourceDeclaration as R, RoleKind};
pub(super) fn installed(slot: &Arc<ComponentSlot>) -> bool {
    let spec = slot.recipe.spec();
    if !spec.roles.contains(&RoleKind::Stream) {
        return false;
    }
    let (mut native, mut borrowed, mut icmp) = (None, None, None);
    for (ordinal, resource) in spec.resources.iter().enumerate() {
        match resource {
            R::ControlledMetadataCarrier {
                endpoint,
                dial,
                max_origins,
                ..
            } if native.is_none() => {
                native = Some((ordinal, endpoint, dial, *max_origins));
            }
            R::ControlledServiceDatagram {
                endpoint,
                dial,
                max_sockets,
            }
            | R::ControlledCarrierPool {
                endpoint,
                dial,
                max_sockets,
                datagram: true,
            } if borrowed.is_none() => {
                borrowed = Some((ordinal, endpoint, dial, *max_sockets));
            }
            R::NativeIcmp { max_sockets } if icmp.is_none() => icmp = Some(*max_sockets),
            R::File { .. } => {}
            _ => return false,
        }
    }
    let (
        Some((native, endpoint, dial, capacity)),
        Some((borrowed, other_endpoint, other_dial, other_capacity)),
    ) = (native, borrowed)
    else {
        return false;
    };
    endpoint == other_endpoint
        && dial == other_dial
        && capacity == other_capacity
        && icmp.is_none_or(|icmp| icmp == capacity)
        && capacity.get().checked_mul(4).is_some_and(|minimum| {
            spec.task_limit
                .is_some_and(|actual| actual.get() >= minimum)
        })
        && slot.original_metadata_owner(native).is_some_and(|owner| {
            owner.instance() == slot.instance_id() && owner.limit() == capacity
        })
        && slot.original_carrier_owner(borrowed).is_some_and(|owner| {
            owner.instance() == slot.instance_id() && owner.limit() == capacity
        })
}
