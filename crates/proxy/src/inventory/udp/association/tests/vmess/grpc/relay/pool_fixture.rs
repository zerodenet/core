//! Observations of actual pooled prefix resources.
use super::*;
pub(super) use crate::runtime::MuxPrefix as Pool;
pub(super) fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
pub(super) fn pool_owner(
    proxy: &Proxy,
    tag: &str,
) -> zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration {
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let slot = plan.configured(tag).unwrap();
    assert_eq!(slot.dependency_instances().len(), 1);
    plan.original_carrier_owner(slot.dependency_instances()[0], 0)
        .unwrap()
}
