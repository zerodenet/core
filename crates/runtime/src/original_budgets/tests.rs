//! Real hosted ResourceAccess over the same Root, without native packet IO.
use super::*;
use std::{num::NonZeroUsize, sync::Arc};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::contract::{
    component::ResourceAccess, scope::TaskScope, CancellationToken,
};
use zero_tun::packet_memory::{PacketMetadataArc, PacketMetadataLayout};

#[tokio::test]
async fn hosted_metadata_views_keep_actual_weak_headers_charged_across_policy_shrink() {
    let roots = Arc::new(HostOriginalBudgets::default());
    let mut runtime = zero_config::RuntimeOptionsConfig::default();
    roots.install_original_limits(&runtime).unwrap();
    let scope = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::MIN);
    let resources = crate::component::instance::HostResources::new_hosted(
        ComponentInstanceId {
            component: ComponentId(7),
            generation: ResourceGeneration(1),
        },
        &[],
        CancellationToken::new(),
        scope.registrar().downgrade(),
        roots.clone(),
    )
    .unwrap();
    let view = resources.packet_driver_metadata().unwrap();
    let sibling = resources.packet_driver_metadata().unwrap();
    let baseline = roots.packet.driver_metadata().occupied;
    let layout = PacketMetadataArc::<[u8; 2048]>::layout().unwrap();
    let value = PacketMetadataArc::from_prepaid([7; 2048], view.reserve(layout).unwrap())
        .ok()
        .unwrap();
    let weak = PacketMetadataArc::downgrade(&value);
    drop(value);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        sibling.snapshot().occupied,
        baseline.checked_add(layout).unwrap()
    );
    assert_eq!(sibling.snapshot(), roots.packet.driver_metadata());
    let unchanged = roots.packet.driver_metadata().limit;
    let prior = runtime.clone();
    runtime.packet_driver_metadata_total_max_bytes = Some(baseline.bytes() + 1);
    assert!(HostOriginalBudgets::limits_changed(&prior, &runtime));
    assert!(roots.install_original_limits(&runtime).is_err());
    assert_eq!(roots.packet.driver_metadata().limit, unchanged);
    drop(weak);
    assert_eq!(view.snapshot().occupied, baseline);
    roots.install_original_limits(&runtime).unwrap();
    assert_eq!(
        view.snapshot().limit,
        PacketMetadataLayout::new(
            runtime.effective_packet_driver_metadata_total_max_allocations(),
            baseline.bytes() + 1
        )
    );
    assert!(view.reserve(layout).is_err());
}
