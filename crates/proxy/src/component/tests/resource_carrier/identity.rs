use super::*;

#[tokio::test]
async fn exact_ordinal_actual_instance_cap_scope_and_start_phase_are_required() {
    let mut fixture = Fixture::new().await;
    assert!(HostResources::new(
        instance().component,
        &[fixture.declaration.clone()],
        CancellationToken::new()
    )
    .is_err());
    let actual = fixture.owner.registration();
    assert!(fixture
        .resources
        .register_controlled_carrier(0, actual.clone(), &fixture.scope.registrar())
        .is_err());
    fixture.scope.enable_registration().unwrap();
    assert!(fixture
        .resources
        .register_controlled_carrier(0, actual.clone(), &fixture.scope.registrar())
        .is_err());
    assert!(!fixture.resources.io_evidence().carriers[0].registered);
    let access = fixture.resources.activate(deadline()).unwrap();
    let mut foreign_scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(one()));
    foreign_scope.enable_registration().unwrap();
    assert!(fixture
        .resources
        .register_controlled_carrier(0, actual.clone(), &foreign_scope.registrar())
        .is_err());
    let wrong_cap = CarrierOwner::new(
        instance(),
        NonZeroUsize::new(2).unwrap(),
        &CancellationToken::new(),
    );
    assert!(fixture
        .resources
        .register_controlled_carrier(0, wrong_cap.registration(), &fixture.scope.registrar())
        .is_err());
    let mut foreign_instance = instance();
    foreign_instance.generation = ResourceGeneration(5);
    let foreign = CarrierOwner::new(foreign_instance, one(), &CancellationToken::new());
    assert!(fixture
        .resources
        .register_controlled_carrier(0, foreign.registration(), &fixture.scope.registrar())
        .is_err());
    assert!(fixture
        .resources
        .register_controlled_carrier(1, actual.clone(), &fixture.scope.registrar())
        .is_err());
    let bound = fixture
        .resources
        .register_controlled_carrier(0, actual.clone(), &fixture.scope.registrar())
        .unwrap();
    assert!(bound.matches_registration(&actual));
    assert!(bound.matches_declaration(&fixture.declaration));
    assert!(bound.scope_identity().matches(&fixture.scope.registrar()));
    let same = fixture
        .resources
        .register_controlled_carrier(0, actual.clone(), &fixture.scope.registrar())
        .unwrap();
    assert!(same.matches_registration(&actual));
    let impostor = CarrierOwner::new(instance(), one(), &CancellationToken::new());
    assert!(fixture
        .resources
        .register_controlled_carrier(0, impostor.registration(), &fixture.scope.registrar())
        .is_err());
    let proof = fixture.resources.io_evidence();
    assert_eq!(proof.declared_count, 1);
    assert!(proof.carriers[0].registered && proof.carriers[0].owner_available);
    assert!(!proof.carriers[0].native_generation_closed && !proof.physical_confirmed());
    drop(access);
    assert!(fixture
        .resources
        .register_controlled_carrier(0, actual, &fixture.scope.registrar())
        .is_err());
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(!fixture
        .resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(fixture.scope.shutdown(deadline()).await.is_complete());
    assert!(foreign_scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn one_original_owner_cannot_cover_two_declarations_and_weak_loss_is_not_closed() {
    let mut fixture = Fixture::new().await;
    let resources = HostResources::new_for_instance(
        instance(),
        &[fixture.declaration.clone(), fixture.declaration.clone()],
        fixture.host_cancel.clone(),
        fixture.scope.registrar().scope_identity(),
    )
    .unwrap();
    fixture.scope.enable_registration().unwrap();
    let _access = resources.activate(deadline()).unwrap();
    let actual = fixture.owner.registration();
    resources
        .register_controlled_carrier(0, actual.clone(), &fixture.scope.registrar())
        .unwrap();
    assert!(resources
        .register_controlled_carrier(1, actual.clone(), &fixture.scope.registrar())
        .is_err());
    let proof = resources.io_evidence();
    assert!(proof.carriers[0].registered);
    assert!(!proof.carriers[1].registered && !proof.physical_confirmed());
    drop(fixture.owner);
    assert!(actual.observe().is_none() && actual.closed().is_none());
    let lost = resources.io_evidence();
    assert!(!lost.carriers[0].owner_available && !lost.carriers[0].native_generation_closed);
    assert!(!lost.physical_confirmed());
    let report = resources.shutdown_io(deadline()).await.unwrap();
    assert_eq!(report.unfinished.len(), 2);
    assert!(report.closed_io.is_empty());
    assert!(fixture.scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn foreign_prebinding_cannot_replace_the_original_host_fence_and_keeps_first_source() {
    let mut fixture = Fixture::new().await;
    fixture.scope.enable_registration().unwrap();
    let actual = fixture.owner.registration();
    let foreign = Arc::new(CancellationToken::new());
    actual
        .bind_declared(&fixture.declaration, &fixture.scope.registrar(), &foreign)
        .unwrap();
    let _access = fixture.resources.activate(deadline()).unwrap();
    assert!(fixture
        .resources
        .register_controlled_carrier(0, actual.clone(), &fixture.scope.registrar())
        .is_err());
    let first = fixture.resources.io_evidence();
    assert!(!first.carriers[0].registered);
    let original = first.carriers[0].first_fault.clone().unwrap();
    assert_eq!(original.reason, Reason::StalePlan);
    assert!(fixture
        .resources
        .register_controlled_carrier(0, actual, &fixture.scope.registrar())
        .is_err());
    let retry = fixture.resources.io_evidence();
    assert!(Arc::ptr_eq(
        &original,
        retry.carriers[0].first_fault.as_ref().unwrap()
    ));
    assert!(!retry.physical_confirmed());
    fixture.owner.shutdown(deadline()).await.unwrap();
    assert!(fixture.resources.shutdown_io(deadline()).await.is_err());
    assert!(Arc::ptr_eq(
        &original,
        fixture.resources.io_evidence().carriers[0]
            .first_fault
            .as_ref()
            .unwrap()
    ));
    fixture.scope.shutdown(deadline()).await;
}

#[tokio::test]
async fn lookup_requires_the_actual_reserved_grant_and_full_published_binding_after_start_phase() {
    let mut fixture = Fixture::new().await;
    fixture.scope.enable_registration().unwrap();
    let access = fixture.resources.activate(deadline()).unwrap();
    fixture
        .resources
        .register_controlled_carrier(0, fixture.owner.registration(), &fixture.scope.registrar())
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let grant = fixture.owner.grants().reserve().unwrap();
    let binding = fixture.transport.binding().clone();
    assert!(fixture
        .resources
        .registered_carrier(
            instance(),
            &fixture.destination(),
            &binding,
            ControlledStreamDemandClass::Service,
            &grant
        )
        .is_err());
    drop(access);
    let actual = fixture
        .resources
        .registered_carrier(
            instance(),
            &fixture.destination(),
            &binding,
            ControlledStreamDemandClass::Service,
            &grant,
        )
        .unwrap();
    assert!(actual.matches_registration(&fixture.owner.registration()));
    let mut wrong = binding.clone();
    wrong.network_generation = NetworkGeneration(wrong.network_generation.0 + 1);
    assert!(fixture
        .resources
        .registered_carrier(
            instance(),
            &fixture.destination(),
            &wrong,
            ControlledStreamDemandClass::Service,
            &grant
        )
        .is_err());
    assert!(fixture
        .resources
        .registered_carrier(
            instance(),
            &fixture.destination(),
            &binding,
            ControlledStreamDemandClass::Business,
            &grant
        )
        .is_err());
    let mut foreign = CarrierOwner::new(instance(), one(), &CancellationToken::new());
    foreign.start(deadline()).await.unwrap();
    let foreign_grant = foreign.grants().reserve().unwrap();
    assert!(fixture
        .resources
        .registered_carrier(
            instance(),
            &fixture.destination(),
            &binding,
            ControlledStreamDemandClass::Service,
            &foreign_grant
        )
        .is_err());
    drop(foreign_grant);
    foreign.shutdown(deadline()).await.unwrap();
    fixture.host_cancel.cancel();
    assert!(fixture
        .resources
        .registered_carrier(
            instance(),
            &fixture.destination(),
            &binding,
            ControlledStreamDemandClass::Service,
            &grant
        )
        .is_err());
    drop(grant);
    fixture.owner.shutdown(deadline()).await.unwrap();
    assert!(!fixture
        .resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .is_complete());
    fixture.scope.shutdown(deadline()).await;
}
