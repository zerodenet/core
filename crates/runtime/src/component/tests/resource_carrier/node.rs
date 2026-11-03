//! A changing network never grants another node, policy or demand class.
use super::*;
#[tokio::test]
async fn node_stream_authority_keeps_exact_endpoint_real_owner_and_business_fence_across_network_generations(
) {
    let mut fixture = Fixture::new().await;
    let endpoint = fixture.destination();
    let declaration = ResourceDeclaration::ControlledNodeStream {
        endpoint: endpoint.clone(),
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
    let registration = fixture.owner.registration();
    let bound = resources
        .register_controlled_carrier(0, registration.clone(), &fixture.scope.registrar())
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    drop(access);
    let mut grant = fixture.owner.grants().reserve().unwrap();
    assert!(
        grant.claim_driver_view().is_err(),
        "node business authority is not background driver authority"
    );
    let view = grant.claim_transferred_view().unwrap();
    let binding = fixture.transport.binding().clone();
    for generation in [
        binding.network_generation,
        NetworkGeneration(binding.network_generation.0 + 1),
    ] {
        let mut current = binding.clone();
        current.network_generation = generation;
        let (actual, _issuer) = resources
            .registered_business_issuer(instance(), &endpoint, &current, &grant)
            .unwrap();
        assert!(actual.matches_registration(&registration));
        assert!(actual.matches_declaration(&declaration));
    }
    for class in [
        ControlledStreamDemandClass::Service,
        ControlledStreamDemandClass::BackgroundControl,
    ] {
        assert!(resources
            .registered_carrier(instance(), &endpoint, &binding, class, &grant)
            .is_err());
    }
    let mut wrong = endpoint.clone();
    wrong.port = wrong.port.checked_add(1).unwrap();
    assert!(resources
        .registered_business_issuer(instance(), &wrong, &binding, &grant)
        .is_err());
    let wrong = Destination::new(Address::Domain("node.invalid".into()), endpoint.port);
    assert!(resources
        .registered_business_issuer(instance(), &wrong, &binding, &grant)
        .is_err());
    let mut wrong = binding.clone();
    wrong.dial.interface = Some("different-interface".into());
    assert!(resources
        .registered_business_issuer(instance(), &endpoint, &wrong, &grant)
        .is_err());
    let mut wrong = binding.clone();
    wrong.prefix.push(zero_core::component::RoleInstanceId {
        component: ComponentId(33),
        role: zero_core::component::RoleId::Outbound(zero_core::component::OutboundId(33)),
        generation: ResourceGeneration(1),
    });
    assert!(resources
        .registered_business_issuer(instance(), &endpoint, &wrong, &grant)
        .is_err());
    assert!(bound
        .node_business_issuer(
            &Arc::new(CancellationToken::new()),
            &grant,
            &endpoint,
            &binding
        )
        .is_err());
    let mut foreign = CarrierOwner::new(instance(), one(), &CancellationToken::new());
    foreign.start(deadline()).await.unwrap();
    let other = foreign.grants().reserve().unwrap();
    assert!(resources
        .registered_business_issuer(instance(), &endpoint, &binding, &other)
        .is_err());
    drop(other);
    foreign.shutdown(deadline()).await.unwrap();
    drop(view);
    drop(grant);
    fixture.owner.shutdown(deadline()).await.unwrap();
    fixture.scope.shutdown(deadline()).await;
    assert!(resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .is_complete());
}
