//! Each installed real Relay record, not a count or reconstructed Scope.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierOriginOwner;
pub(super) fn layers(
    proxy: &Proxy,
    call: &zero_dns::OwnedDnsQueryObservation,
) -> Vec<CarrierOrigin> {
    let child = call.original.as_ref().expect("actual DNS original");
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let mut current = child.parent();
    let mut layers = Vec::new();
    for index in (0..3).rev() {
        let origin = current.expect("actual previous-role loan in the same Relay");
        let slot = plan.configured(&format!("node-{index}")).unwrap();
        assert_eq!(origin.instance(), slot.instance_id());
        let CarrierOriginOwner::Carrier(owner) = origin.owner() else {
            panic!("actual Node original");
        };
        assert_eq!(owner.limit().get(), 1);
        let driver = origin.driver().expect("actual same-role H2 driver");
        assert!(!driver.is_joined() && origin.closed().is_none());
        assert!(!driver
            .scope_identity()
            .same_scope(&call.task.scope_identity()));
        assert!(driver
            .scope_identity()
            .same_scope(&owner.bound().unwrap().scope_identity()));
        let observed = owner.observe().unwrap();
        assert_eq!(observed.retained(), 1);
        let record = observed
            .slots()
            .iter()
            .find(|record| {
                record
                    .driver()
                    .is_some_and(|(_, actual)| actual.same_registration(driver))
            })
            .unwrap();
        assert!(record.physical_view_live() && record.acquisition_unknown().is_none());
        assert_eq!(record.native().is_some(), index == 0);
        if index == 0 {
            assert!(record.borrowed_origin().is_none() && origin.parent().is_none());
        } else {
            assert!(record
                .borrowed_origin()
                .unwrap()
                .same_original(origin.parent().unwrap()));
        }
        let setup = record
            .auxiliary()
            .expect("actual sequential Node preparation");
        assert!(setup.scope_identity().same_scope(&driver.scope_identity()));
        let joined = setup
            .try_joined()
            .expect("setup precedes this real driver delivery");
        assert!(setup.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
        for other in &layers {
            let other: &CarrierOrigin = other;
            assert!(!driver
                .scope_identity()
                .same_scope(&other.driver().unwrap().scope_identity()));
        }
        layers.push(origin.clone());
        current = origin.parent();
    }
    assert!(current.is_none());
    assert!(child.native_origin().same_original(layers.last().unwrap()));
    layers
}
pub(super) async fn returned(child: &CarrierOrigin, layers: &[CarrierOrigin]) {
    tokio::time::timeout_at(deadline(), async {
        loop {
            if child.closed().is_some()
                && layers.iter().all(|origin| {
                    origin.closed().is_some()
                        && origin
                            .driver()
                            .unwrap()
                            .try_joined()
                            .is_some_and(|joined| joined.registration_released())
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let closed = child.closed().unwrap();
    assert!(closed.matches(child) && !closed.had_native_acquisition());
    for (index, origin) in layers.iter().enumerate() {
        let closed = origin.closed().unwrap();
        assert!(closed.matches(origin));
        assert_eq!(closed.had_native_acquisition(), index == 2);
        let driver = origin.driver().unwrap();
        let joined = driver.try_joined().unwrap();
        assert!(driver.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
        let CarrierOriginOwner::Carrier(owner) = origin.owner() else {
            unreachable!()
        };
        assert!(
            owner.observe().is_some_and(|actual| actual.retained() == 0)
                || owner.closed().is_some_and(|closed| closed.matches(owner))
        );
    }
}
pub(super) fn owner_closed(layers: &[CarrierOrigin]) {
    for original in layers {
        assert!(original.owner().closed().unwrap().matches(original.owner()));
        assert!(original
            .owner()
            .users_closed()
            .unwrap()
            .matches(original.owner()));
    }
}
