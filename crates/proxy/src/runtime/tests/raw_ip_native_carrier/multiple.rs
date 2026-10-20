//! No numeric cap or equal public instance can fabricate a physical owner.
use super::*;

#[tokio::test]
async fn real_same_owner_capacity_accepts_two_original_slots_and_same_slot_observation_is_idempotent(
) {
    let mut fixture = Fixture::new().await;
    let physical = fixture.carrier.registration();
    assert_eq!(physical.limit().get(), 2);
    let first = fixture.carrier.grants().reserve().unwrap();
    let second = fixture.carrier.grants().reserve().unwrap();
    fixture
        .registration
        .observe_carrier_with_owner(&physical, first.adoption())
        .unwrap();
    fixture
        .registration
        .observe_carrier_with_owner(&physical, second.adoption())
        .unwrap();
    fixture
        .registration
        .observe_carrier_with_owner(&physical, first.adoption())
        .unwrap();
    fixture
        .registration
        .observe_carrier_with_owner(&physical, second.adoption())
        .unwrap();
    assert!(
        fixture.carrier.grants().reserve().is_err(),
        "the sole original owner enforces its actual declared capacity"
    );
    {
        let state = fixture.native.0.lock().unwrap();
        let record = state.records.get(&fixture.registration.id).unwrap();
        assert!(record.carrier_owner.as_ref().unwrap().same_owner(&physical));
        assert_eq!(record.additional_carriers.len(), 1);
        assert!(!record.carrier_conflict);
    }
    assert!(fixture.registration.carrier_completion().is_none());
    drop((first, second));
    fixture.join().await;
    fixture
        .carrier
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(
        fixture.registration.carrier_completion().is_none(),
        "two unused receipts are not actual native closure"
    );
    fixture.assert_gate_retained().await;
}

#[tokio::test]
async fn equal_public_instance_with_a_different_original_owner_is_rejected_and_stays_unknown() {
    let mut fixture = Fixture::new().await;
    let mut foreign = CarrierOwner::new(
        fixture.carrier.instance(),
        NonZeroUsize::new(2).unwrap(),
        &CancellationToken::new(),
    );
    foreign
        .start(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let first = fixture.carrier.grants().reserve().unwrap();
    let other = foreign.grants().reserve().unwrap();
    fixture
        .registration
        .observe_carrier_with_owner(&fixture.carrier.registration(), first.adoption())
        .unwrap();
    assert_eq!(
        fixture
            .registration
            .observe_carrier_with_owner(&foreign.registration(), other.adoption())
            .unwrap_err()
            .reason,
        Reason::StalePlan
    );
    assert_eq!(
        fixture
            .registration
            .observe_carrier_with_owner(&fixture.carrier.registration(), first.adoption())
            .unwrap_err()
            .reason,
        Reason::InvalidConfig
    );
    {
        let state = fixture.native.0.lock().unwrap();
        let record = state.records.get(&fixture.registration.id).unwrap();
        assert!(record
            .carrier
            .as_ref()
            .unwrap()
            .same_slot(&first.adoption()));
        assert!(record.additional_carriers.is_empty());
        assert!(record.carrier_conflict);
    }
    drop((first, other));
    fixture.join().await;
    fixture
        .carrier
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    foreign
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(fixture.registration.carrier_completion().is_none());
    fixture.assert_gate_retained().await;
}

#[tokio::test]
async fn explicitly_closed_unused_original_can_leave_the_bounded_observer_without_clearing_gateway()
{
    let mut fixture = Fixture::new().await;
    let physical = fixture.carrier.registration();
    let first = fixture.carrier.grants().reserve().unwrap();
    let unused = first.adoption();
    let second = fixture.carrier.grants().reserve().unwrap();
    fixture
        .registration
        .observe_carrier_with_owner(&physical, unused.clone())
        .unwrap();
    fixture
        .registration
        .observe_carrier_with_owner(&physical, second.adoption())
        .unwrap();
    drop(first);
    unused
        .retire(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(!unused.closed().unwrap().had_native_acquisition());
    let next = fixture.carrier.grants().reserve().unwrap();
    fixture
        .registration
        .observe_carrier_with_owner(&physical, next.adoption())
        .unwrap();
    {
        let state = fixture.native.0.lock().unwrap();
        let record = state.records.get(&fixture.registration.id).unwrap();
        assert_eq!(record.additional_carriers.len(), 1);
        assert!(!record.carrier.as_ref().unwrap().same_slot(&unused));
        assert!(!record
            .additional_carriers
            .iter()
            .any(|slot| slot.same_slot(&unused)));
        assert!(!record.carrier_conflict);
    }
    drop((second, next));
    fixture.join().await;
    fixture
        .carrier
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(fixture.registration.carrier_completion().is_none());
    fixture.assert_gate_retained().await;
}

mod overflow;
