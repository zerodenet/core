use super::*;
#[tokio::test]
async fn service_datagram_declaration_never_grants_root_native_or_cached_datagram_adoption() {
    timeout(Duration::from_secs(3), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let endpoint = destination(remote);
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let host = Arc::new(CancellationToken::new());
        let one = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        let mut owner = CarrierOwner::new(instance(), one, &host);
        let registration = owner.registration();
        registration
            .bind_declared(
                &ResourceDeclaration::ControlledServiceDatagram {
                    endpoint: endpoint.clone(),
                    dial: Default::default(),
                    max_sockets: one,
                },
                &scope.registrar(),
                &host,
            )
            .unwrap();
        owner.start(deadline()).await.unwrap();
        let mut grant = owner.grants().reserve().unwrap();
        let view = grant.claim_transferred_view().unwrap();
        let original = grant.adoption();
        let opener =
            NativeCarrierOpener::new(instance(), binding.clone(), remote, egress, None).unwrap();
        let denied = opener
            .prepare_controlled_datagram(&binding, endpoint, context(), grant)
            .err()
            .expect("a Service receiver is not root native authority");
        assert_eq!(
            (denied.stage, denied.reason, denied.delivery),
            (
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted
            )
        );
        drop(view);
        assert!(original.retire(deadline()).await.unwrap().is_complete());
        let closed = original.closed().unwrap();
        assert!(closed.matches(&original) && !closed.had_native_acquisition());
        assert!(original.users_closed().unwrap().matches(&original));
        let mut bytes = [0; 1];
        let mut receive = Box::pin(peer.recv_from(&mut bytes));
        poll_fn(|cx| {
            assert!(receive.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(receive);
        assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
        let closed = scope.shutdown(deadline()).await;
        assert!(closed.is_complete() && closed.exited_tasks.is_empty());
    })
    .await
    .unwrap();
}
