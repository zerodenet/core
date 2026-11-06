//! Actual original controls are independent of compact CloseReport batch rows.
use crate::Proxy;
use zero_platform_tokio::controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration};
pub(super) struct Original {
    owner: CarrierOwnerRegistration,
    original: CarrierOriginObservation,
}
impl Original {
    pub(super) fn capture(proxy: &Proxy, tag: Option<&str>) -> Self {
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let slot = match tag {
            Some(tag) => plan
                .configured(tag)
                .expect("same actual configured outbound"),
            None => plan.direct(None).unwrap(),
        };
        for ordinal in 0..slot.recipe.spec().resources.len() {
            let Some(owner) = slot.original_carrier_owner(ordinal) else {
                continue;
            };
            let observed = owner.observe().expect("actual live original provider");
            if let Some(original) = observed.slots().iter().find(|original| {
                original.original().closed().is_none()
                    && original.native().is_some_and(|native| !native.is_closed())
            }) {
                assert!(original.acquisition_unknown().is_none());
                assert!(original.native().unwrap().disposition_unknown().is_none());
                return Self {
                    original: original.original().clone(),
                    owner,
                };
            }
        }
        panic!("actual admitted wire must retain its same outbound native original");
    }
    pub(super) fn assert_native_closed(&self) {
        let closed = self
            .original
            .closed()
            .expect("same actual native original receipt");
        assert!(self.original.matches_closed(&closed));
        assert!(closed.had_native_acquisition());
    }
    pub(super) fn assert_owner_closed(&self) {
        self.assert_native_closed();
        assert!(
            self.owner
                .closed()
                .is_some_and(|closed| closed.matches(&self.owner)),
            "actual outbound host slot native closure"
        );
        assert!(
            self.owner
                .users_closed()
                .is_some_and(|closed| closed.matches(&self.owner)),
            "same outbound transferred users confirmed independently"
        );
    }
}
pub(super) fn accepted(owner: &crate::runtime::inbound_owners::InboundOwner) -> u64 {
    let (claim, closed) = owner
        .accepted_closed_for_test()
        .expect("actual Source consumed its same accepted native/users/Scope closure");
    assert!(closed.matches(&claim));
    assert_eq!(closed.id(), claim.id());
    closed.id()
}
pub(super) fn source_closed(
    owner: &crate::runtime::inbound_owners::InboundOwner,
    at_least_tasks: usize,
) {
    let closed = owner
        .terminal_for_test()
        .expect("actual Source native, users, resources and Scope all closed");
    assert_eq!(closed.identity(), owner.identity());
    assert!(closed.failure().is_none());
    assert!(closed.task_failures().is_empty());
    assert!(closed.native_bound_failures().is_empty());
    assert!(closed.joined_tasks() >= at_least_tasks);
}
