//! Real transferred Direct preparation, with same Auxiliary and deferred ACK.
use super::*;
#[tokio::test]
async fn prepared_direct_service_keeps_transferred_view_and_waits_real_auxiliary_ack_before_delivery(
) {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let fence = Arc::new(CancellationToken::new());
        let bound = fixture
            .owner
            .registration()
            .bind_declared(
                &crate::contract::component::ResourceDeclaration::ControlledDirectStream {
                    dial: fixture.transport.binding().dial.clone(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &tasks,
                &fence,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let original = grant.adoption();
        let (view, preparation) = grant.claim_preparing_transferred_view().unwrap();
        let (foreign_release, foreign_wait) = tokio::sync::oneshot::channel();
        let (_, foreign) = tasks
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let _ = foreign_wait.await;
                Ok(())
            })
            .unwrap();
        assert!(bound
            .prepared_direct_service_issuer(
                &fence,
                &grant,
                foreign.clone(),
                &fixture.destination,
                fixture.transport.binding()
            )
            .is_err());
        let (begin, started) =
            tokio::sync::oneshot::channel::<crate::contract::scope::TaskJoinWitness>();
        let (returned, ready) = tokio::sync::oneshot::channel();
        let binding = fixture.transport.binding().clone();
        let endpoint = fixture.destination.clone();
        let context = context();
        let selection = fixture.egress.clone();
        let (_, actual) = original
            .spawn_auxiliary(&tasks, async move {
                let actual = started.await.unwrap();
                assert!(bound
                    .prepared_direct_service_issuer(
                        &Arc::new(CancellationToken::new()),
                        &grant,
                        actual.clone(),
                        &endpoint,
                        &binding
                    )
                    .is_err());
                let mut changed = binding.clone();
                changed.prefix.push(zero_core::component::RoleInstanceId {
                    component: instance().component,
                    generation: instance().generation,
                    role: zero_core::component::RoleId::Outbound(zero_core::component::OutboundId(
                        instance().component.0,
                    )),
                });
                assert!(bound
                    .prepared_direct_service_issuer(
                        &fence,
                        &grant,
                        actual.clone(),
                        &endpoint,
                        &changed
                    )
                    .is_err());
                assert!(bound
                    .prepared_direct_service_issuer(&fence, &grant, foreign, &endpoint, &binding)
                    .is_err());
                let cancelled = CancellationToken::new();
                cancelled.cancel();
                assert!(bound
                    .prepared_direct_service_issuer(
                        &fence,
                        &grant,
                        actual.clone(),
                        &endpoint,
                        &binding
                    )
                    .unwrap()
                    .authorize(cancelled, context.deadline)
                    .is_err());
                let ticket = bound
                    .prepared_direct_service_issuer(
                        &fence,
                        &grant,
                        actual.clone(),
                        &endpoint,
                        &binding,
                    )
                    .unwrap()
                    .authorize(context.cancel.clone(), context.deadline)
                    .unwrap();
                {
                    let mut state = grant.slot.state();
                    let users = state.users.as_mut().unwrap();
                    assert!(users.transferred() && users.live());
                    assert!(
                        !users.caller_matches(&actual),
                        "no Business caller was bound"
                    );
                    assert!(
                        !users.bind(actual.clone()),
                        "Users::bind still rejects transferred custody"
                    );
                }
                let native = NativeCarrierOpener::service(ticket, selection, None).unwrap();
                let carrier = native
                    .prepare_controlled_stream(&binding, endpoint, context, grant)
                    .unwrap()
                    .open(deadline())
                    .await
                    .unwrap();
                let _ = returned.send(carrier);
                Ok(())
            })
            .unwrap();
        begin.send(actual.clone()).unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let carrier = ready.await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(actual.matches_joined(receipt.joined()));
        assert!(!receipt.joined().registration_released());
        assert!(original.accept_auxiliary().await.unwrap().is_some());
        assert!(preparation.publish_transferred().is_err());
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        scope.acknowledge(&receipt).unwrap();
        preparation.publish_transferred().unwrap();
        let (mut io, _) = carrier.into_parts();
        io.write_all(b"prepared original").await.unwrap();
        let mut bytes = [0; 17];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"prepared original");
        io.shutdown().await.unwrap();
        let mut end = [0; 1];
        assert_eq!(peer.read(&mut end).await.unwrap(), 0);
        peer.write_all(b"same native reply").await.unwrap();
        peer.shutdown().await.unwrap();
        let mut response = Vec::new();
        io.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"same native reply");
        assert!(original.closed().is_none());
        drop(io);
        drop(view);
        assert!(original.retire(deadline()).await.unwrap().is_complete());
        assert!(original.closed().unwrap().matches(&original));
        assert!(original.users_closed().unwrap().matches(&original));
        foreign_release.send(()).unwrap();
        let foreign_receipt = scope.next_receipt().await.unwrap();
        scope.acknowledge(&foreign_receipt).unwrap();
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
