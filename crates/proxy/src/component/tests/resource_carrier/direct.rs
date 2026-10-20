//! Dynamic Direct is demand-bound Business, never a wildcard fixed endpoint.
use super::*;
use zero_platform_tokio::contract::lifecycle::TaskClass;

#[tokio::test]
async fn direct_registration_uses_original_ordinal_owner_fence_and_exact_per_demand_facts() {
    let mut fixture = Fixture::new().await;
    let declaration = ResourceDeclaration::ControlledDirectStream {
        dial: Default::default(),
        max_sockets: one(),
    };
    let resources = HostResources::new_for_instance(
        instance(),
        &[declaration.clone()],
        fixture.host_cancel.clone(),
        fixture.scope.registrar().scope_identity(),
    )
    .unwrap();
    fixture.scope.enable_registration().unwrap();
    let access = resources.activate(deadline()).unwrap();
    let actual = fixture.owner.registration();
    let bound = resources
        .register_controlled_carrier(0, actual.clone(), &fixture.scope.registrar())
        .unwrap();
    assert!(bound.matches_declaration(&declaration));
    assert!(bound.fixed_endpoint_binding().is_none());
    fixture.owner.start(deadline()).await.unwrap();
    drop(access);
    let grant = fixture.owner.grants().reserve().unwrap();
    let binding = fixture.transport.binding().clone();
    let target = fixture.destination();
    assert!(resources
        .registered_carrier(
            instance(),
            &target,
            &binding,
            ControlledStreamDemandClass::Service,
            &grant
        )
        .is_err());
    assert!(resources
        .registered_carrier(
            instance(),
            &target,
            &binding,
            ControlledStreamDemandClass::BackgroundControl,
            &grant
        )
        .is_err());
    let selected = resources
        .registered_carrier(
            instance(),
            &target,
            &binding,
            ControlledStreamDemandClass::Business,
            &grant,
        )
        .unwrap();
    assert!(selected.matches_registration(&actual));
    assert!(bound
        .business_issuer(&Arc::new(CancellationToken::new()), &grant)
        .is_err());
    let (_, _issuer) = resources
        .registered_business_issuer(instance(), &target, &binding, &grant)
        .unwrap();
    let mut wrong_dial = binding.clone();
    wrong_dial.dial.interface = Some("different-interface".into());
    assert!(resources
        .registered_business_issuer(instance(), &target, &wrong_dial, &grant)
        .is_err());
    let mut relayed = binding.clone();
    relayed.prefix.push(zero_core::component::RoleInstanceId {
        component: ComponentId(123),
        role: zero_core::component::RoleId::Outbound(zero_core::component::OutboundId(1)),
        generation: ResourceGeneration(1),
    });
    assert!(resources
        .registered_business_issuer(instance(), &target, &relayed, &grant)
        .is_err());
    let domain = Destination::new(Address::Domain("example.invalid".into()), target.port);
    assert!(resources
        .registered_business_issuer(instance(), &domain, &binding, &grant)
        .is_err());
    let zero_port = Destination::new(Address::Ipv4([127, 0, 0, 1]), 0);
    assert!(resources
        .registered_business_issuer(instance(), &zero_port, &binding, &grant)
        .is_err());
    let mut foreign = CarrierOwner::new(instance(), one(), &CancellationToken::new());
    foreign.start(deadline()).await.unwrap();
    let foreign_grant = foreign.grants().reserve().unwrap();
    assert!(resources
        .registered_business_issuer(instance(), &target, &binding, &foreign_grant)
        .is_err());
    drop(foreign_grant);
    foreign.shutdown(deadline()).await.unwrap();
    // No native acquisition occurs: explicit shutdown is still required for
    // both the original owner's native and NoTransfer receipts.
    assert!(!resources.io_evidence().physical_confirmed());
    drop(grant);
    fixture.owner.shutdown(deadline()).await.unwrap();
    let proof = resources.io_evidence();
    assert!(proof.physical_confirmed());
    assert_eq!(proof.declarations[0].kind, crate::component::instance::resources::native::coverage::DeclaredResourceKind::ControlledDirectStream);
    let mut confused = proof.clone();
    confused.declarations[0].kind = crate::component::instance::resources::native::coverage::DeclaredResourceKind::ControlledOutboundStream;
    assert!(
        !confused.physical_confirmed(),
        "a Direct witness cannot cover the fixed descriptor kind"
    );
    assert!(resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .is_complete());
    fixture.scope.shutdown(deadline()).await;
}

#[tokio::test]
async fn actual_zero_task_scope_can_register_resource_without_inventing_a_task_budget() {
    let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
    let declaration = ResourceDeclaration::ControlledDirectStream {
        dial: Default::default(),
        max_sockets: one(),
    };
    let resources = HostResources::new_for_instance(
        instance(),
        &[declaration],
        CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    let mut owner = CarrierOwner::new(instance(), one(), &CancellationToken::new());
    scope.enable_registration().unwrap();
    assert!(scope.registrar().scope_identity().resource_open());
    assert!(!scope.registrar().scope_identity().registration_open());
    let access = resources.activate(deadline()).unwrap();
    resources
        .register_controlled_carrier(0, owner.registration(), &scope.registrar())
        .unwrap();
    assert_eq!(
        scope
            .registrar()
            .spawn(TaskClass::Auxiliary, async { Ok(()) })
            .unwrap_err(),
        zero_platform_tokio::contract::scope::SpawnError::Capacity
    );
    drop(access);
    owner.shutdown(deadline()).await.unwrap();
    assert!(resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
