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

pub(super) fn pending_suffix_loan(
    proxy: &Proxy,
    pool: &zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration,
    receiver: &zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration,
) -> zero_platform_tokio::controlled_carrier::CarrierOriginObservation {
    let waiting = receiver.observe().unwrap();
    let [reserved] = waiting.slots() else {
        panic!("one actual final Datagram reservation");
    };
    assert!(reserved.driver().is_none() && reserved.native().is_none());
    assert!(reserved.borrowed_origin().is_none() && !reserved.physical_view_live());
    assert!(reserved.original().closed().is_none());

    // The suffix is still awaiting its real CONNECT response. It holds the
    // donor loan until it can deliver that stream to the final receiver.
    let suffix = donor_owner(proxy, "suffix").observe().unwrap();
    let [held] = suffix.slots() else {
        panic!("one actual SOCKS suffix original");
    };
    assert!(held.physical_view_live() && held.native().is_none());
    assert!(held.borrowed_origin().unwrap().owner().same_owner(pool));
    let original = held.original().clone();
    assert!(original.closed().is_none());
    original
}
