//! Dynamic UDP requires its own actual same-Scope sealed ticket.
use super::*;
use zero_platform_tokio::contract::{lifecycle::TaskClass, network::UpstreamOpener};
#[tokio::test]
async fn direct_dual_owner_rejects_bare_and_stream_authority_before_native_udp_acquisition() {
    let mut fixture = Fixture::new().await;
    let declaration = ResourceDeclaration::ControlledDirectCarrier {
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
    resources
        .register_controlled_carrier(0, fixture.owner.registration(), &fixture.scope.registrar())
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    drop(access);
    let binding = fixture.transport.binding().clone();
    let endpoint = fixture.destination();
    let address = fixture.listener.local_addr().unwrap();
    let socket = tokio::net::UdpSocket::bind(address).await.unwrap();
    let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
    let opener =
        NativeCarrierOpener::new(instance(), binding.clone(), address, egress.clone(), None)
            .unwrap();
    let mut grant = fixture.owner.grants().reserve().unwrap();
    let view = grant.claim_transferred_view().unwrap();
    assert!(
        opener
            .prepare_controlled_datagram(&binding, endpoint.clone(), fixture.context(), grant)
            .is_err(),
        "a dynamic descriptor and bare opener grant no arbitrary UDP authority"
    );
    drop(view);
    fixture
        .owner
        .grants()
        .reap_returned(deadline())
        .await
        .unwrap();
    assert_eq!(fixture.owner.retained(), 0);
    let mut grant = fixture.owner.grants().reserve().unwrap();
    let (view, _preparation) = grant.claim_preparing_transferred_view().unwrap();
    let adoption = grant.adoption();
    let actual = resources.clone();
    let context = fixture.context();
    let (begin, start) = tokio::sync::oneshot::channel();
    let (done, received) = tokio::sync::oneshot::channel();
    let (_, witness) = adoption
        .spawn_auxiliary_weak(&fixture.scope.registrar().downgrade(), async move {
            let caller = start.await.unwrap();
            let issuer = actual
                .registered_direct_background_issuer(
                    instance(),
                    &endpoint,
                    &binding,
                    &grant,
                    caller,
                )
                .unwrap();
            let stream_ticket = issuer
                .authorize(context.cancel.clone(), context.deadline)
                .unwrap();
            let opener = NativeCarrierOpener::service(stream_ticket, egress, None).unwrap();
            assert!(
                opener
                    .prepare_controlled_datagram(&binding, endpoint, context, grant)
                    .is_err(),
                "same-slot Stream permission cannot be used as Datagram permission"
            );
            done.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    begin.send(witness.clone()).unwrap();
    tokio::time::timeout_at(deadline(), received)
        .await
        .unwrap()
        .unwrap();
    assert!(witness
        .scope_identity()
        .same_scope(&fixture.scope.registrar().scope_identity()));
    drop(view);
    let (joined, closed_scope) = tokio::join!(
        adoption.accept_auxiliary(),
        fixture.scope.shutdown(deadline())
    );
    assert!(closed_scope.is_complete());
    let joined = joined.unwrap().unwrap();
    assert_eq!(joined.class(), TaskClass::Aux);
    assert!(witness.matches_joined(&joined));
    joined.result().unwrap();
    assert!(fixture.owner.registration().observe().unwrap().slots()[0]
        .native()
        .is_none());
    let ack = witness.wait_registration_released().await;
    assert!(witness.matches_joined(&ack) && ack.registration_released());
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert_eq!(fixture.owner.retained(), 0);
    std::future::poll_fn(|cx| {
        assert!(fixture.listener.poll_accept(cx).is_pending());
        let mut bytes = [0; 1];
        let mut target = tokio::io::ReadBuf::new(&mut bytes);
        assert!(socket.poll_recv_from(cx, &mut target).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(resources
        .shutdown_io(deadline())
        .await
        .unwrap()
        .is_complete());
}
