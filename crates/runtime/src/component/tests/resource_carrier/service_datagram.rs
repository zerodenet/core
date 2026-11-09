//! Same existing Service-datagram declaration reaches actual Host registration.
use super::*;
#[tokio::test]
async fn service_datagram_registration_keeps_exact_ordinal_scope_owner_and_no_cached_driver_permission(
) {
    let mut fixture = Fixture::new().await;
    let declaration = ResourceDeclaration::ControlledServiceDatagram {
        endpoint: fixture.destination(),
        dial: Default::default(),
        max_sockets: one(),
    };
    let resources = HostResources::new_for_instance(
        instance(),
        std::slice::from_ref(&declaration),
        fixture.host_cancel.clone(),
        fixture.scope.registrar().scope_identity(),
    )
    .unwrap();
    fixture.scope.enable_registration().unwrap();
    let access = resources.activate(deadline()).unwrap();
    let actual = fixture.owner.registration();
    assert!(resources
        .register_controlled_carrier(1, actual.clone(), &fixture.scope.registrar())
        .is_err());
    let foreign = CarrierOwner::new(
        ComponentInstanceId {
            component: ComponentId(instance().component.0 + 1),
            generation: instance().generation,
        },
        one(),
        &CancellationToken::new(),
    );
    assert!(resources
        .register_controlled_carrier(0, foreign.registration(), &fixture.scope.registrar())
        .is_err());
    let wrong_scope = TaskScope::new(tokio::runtime::Handle::current(), one());
    assert!(resources
        .register_controlled_carrier(0, actual.clone(), &wrong_scope.registrar())
        .is_err());
    let bound = resources
        .register_controlled_carrier(0, actual.clone(), &fixture.scope.registrar())
        .unwrap();
    assert!(bound.matches_registration(&actual) && bound.matches_declaration(&declaration));
    assert_eq!(
        bound.demand(),
        ControlledStreamDemandClass::BackgroundControl
    );
    assert!(!bound.supports_business());
    fixture.owner.start(deadline()).await.unwrap();
    drop(access);
    let mut grant = fixture.owner.grants().reserve().unwrap();
    assert!(
        grant.claim_driver_view().is_err(),
        "no driver-retained IO authority was added"
    );
    let view = grant.claim_transferred_view().unwrap();
    let original = grant.adoption();
    drop(grant);
    assert_eq!(
        fixture.owner.grants().reserve().err().unwrap().reason,
        Reason::Capacity
    );
    drop(view);
    let report = original.retire(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(
        original.closed().unwrap().matches(&original)
            && original.users_closed().unwrap().matches(&original)
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    fixture.scope.begin_shutdown();
    assert!(fixture.scope.is_drained());
}
