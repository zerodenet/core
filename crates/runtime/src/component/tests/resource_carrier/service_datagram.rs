//! Same existing Service-datagram declaration reaches actual Host registration.
use super::*;
use zero_core::component::{AssociationId, OutboundId, RoleId, RoleInstanceId};
use zero_platform_tokio::contract::{network::UpstreamOpener, scope::TaskClass};
#[tokio::test]
async fn service_datagram_registration_keeps_exact_ordinal_scope_owner_and_no_cached_driver_permission(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
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
        // Unique driver-view custody is allowed. Cached-pool retention is a
        // separate authority: test it after installing the real borrowed driver.
        let peer = tokio::net::UdpSocket::bind(fixture.listener.local_addr().unwrap())
            .await
            .unwrap();
        let endpoint = fixture.destination();
        let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: Vec::new(),
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let donor_id = ComponentInstanceId {
            component: ComponentId(instance().component.0 + 2),
            generation: instance().generation,
        };
        let mut donor = CarrierOwner::new(donor_id, one(), &fixture.host_cancel);
        donor
            .registration()
            .bind_declared(
                &ResourceDeclaration::ControlledDatagramCarrier {
                    endpoint: endpoint.clone(),
                    binding: binding.clone(),
                    max_sockets: one(),
                },
                &fixture.scope.registrar(),
                &Arc::new(fixture.host_cancel.clone()),
            )
            .unwrap();
        donor.start(deadline()).await.unwrap();
        let mut donor_grant = donor.grants().reserve().unwrap();
        let donor_view = donor_grant.claim_transferred_view().unwrap();
        let donor_original = donor_grant.adoption();
        let opener = NativeCarrierOpener::new(
            donor_id,
            binding.clone(),
            peer.local_addr().unwrap(),
            egress,
            None,
        )
        .unwrap();
        let native = opener
            .prepare_controlled_datagram(&binding, endpoint.clone(), fixture.context(), donor_grant)
            .unwrap();
        let local = native.local_addr().unwrap();
        let loan = native
            .into_datagram_loan(donor_view, AssociationId(981))
            .unwrap();
        let mut borrowed_binding = binding;
        borrowed_binding.prefix.push(RoleInstanceId {
            component: donor_id.component,
            role: RoleId::Outbound(OutboundId(1)),
            generation: donor_id.generation,
        });
        let mut grant = fixture.owner.grants().reserve().unwrap();
        let view = grant.claim_driver_view().unwrap();
        assert!(
            grant.claim_transferred_view().is_err(),
            "the view stays unique"
        );
        let original = grant.adoption();
        let io = grant
            .adopt_service_datagram_loan(&borrowed_binding, endpoint, &fixture.context(), loan)
            .unwrap();
        let (ready, observed) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let (id, witness) = original
            .spawn_driver(
                &fixture.scope.registrar(),
                TaskClass::Critical,
                io.into_driver(),
                |io| async move {
                    let _view = view;
                    io.send(b"actual service driver").await.unwrap();
                    let mut bytes = [0; 64];
                    let size = io.receive(&mut bytes).await.unwrap();
                    assert_eq!(&bytes[..size], b"actual service driver");
                    ready.send(()).unwrap();
                    let _ = released.await;
                    Ok(())
                },
            )
            .unwrap();
        let mut bytes = [0; 64];
        let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(source, local);
        assert_eq!(&bytes[..size], b"actual service driver");
        peer.send_to(&bytes[..size], source).await.unwrap();
        observed.await.unwrap();
        assert!(!witness.is_joined());
        let actual_driver = actual.observe().unwrap();
        assert!(actual_driver.slots()[0]
            .driver()
            .is_some_and(|(actual_id, actual_witness)| {
                *actual_id == id && actual_witness.same_registration(&witness)
            }));
        assert!(
            original.retained_driver().is_none(),
            "no driver-retained IO authority was added despite the real registered UDP driver"
        );
        assert_eq!(
            fixture.owner.grants().reserve().err().unwrap().reason,
            Reason::Capacity
        );
        assert!(original.closed().is_none() && donor_original.closed().is_none());
        release.send(()).unwrap();
        let exit = fixture.scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        assert!(exit.result.is_ok() && witness.is_joined());
        let report = original.retire(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert!(
            original.closed().unwrap().matches(&original)
                && original.users_closed().unwrap().matches(&original)
        );
        assert!(donor_original.closed().unwrap().had_native_acquisition());
        assert!(donor_original
            .users_closed()
            .unwrap()
            .matches(&donor_original));
        assert!(donor.shutdown(deadline()).await.unwrap().is_complete());
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(fixture.scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
