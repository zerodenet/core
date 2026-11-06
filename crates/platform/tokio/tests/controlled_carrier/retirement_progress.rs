use super::*;
use crate::contract::network::UpstreamOpener;

#[tokio::test]
async fn inline_cancelled_original_waits_for_real_caller_join_before_refunding_its_only_slot() {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let fence = Arc::new(CancellationToken::new());
        let original = fixture.owner.registration();
        let bound = original
            .bind_declared(
                &crate::contract::component::ResourceDeclaration::ControlledDirectStream {
                    dial: fixture.transport.binding().dial.clone(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &fence,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let (begin, started) = tokio::sync::oneshot::channel::<(
            NativeCarrierOpener,
            UpstreamBinding,
            Destination,
            CarrierGrant,
            CarrierUserView,
            CarrierContext,
        )>();
        let (returned, returning) = tokio::sync::oneshot::channel();
        let (finish, finishing) = tokio::sync::oneshot::channel();
        let (id, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let (native, binding, endpoint, grant, user, context) = started.await.unwrap();
                let connected = native
                    .prepare_controlled_stream(&binding, endpoint, context, grant)
                    .unwrap()
                    .open(deadline())
                    .await
                    .unwrap();
                let (mut io, _) = connected.into_parts();
                io.write_all(b"real request").await.unwrap();
                io.shutdown().await.unwrap();
                let mut reply = Vec::new();
                io.read_to_end(&mut reply).await.unwrap();
                assert_eq!(reply, b"real response");
                drop((io, user));
                returned.send(()).unwrap();
                finishing.await.unwrap();
                Ok(())
            })
            .unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let adoption = grant.adoption();
        let slot_cancel = grant.cancellation();
        let user = grant.claim_user_view().unwrap();
        let context = context();
        let ticket = bound
            .direct_service_issuer(
                &fence,
                &grant,
                witness.clone(),
                &fixture.destination,
                fixture.transport.binding(),
            )
            .unwrap()
            .authorize(context.cancel.clone(), context.deadline)
            .unwrap();
        let native = NativeCarrierOpener::service(ticket, fixture.egress.clone(), None).unwrap();
        begin
            .send((
                native,
                fixture.transport.binding().clone(),
                fixture.destination.clone(),
                grant,
                user,
                context,
            ))
            .unwrap_or_else(|_| panic!("actual initializer"));
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let mut request = Vec::new();
        peer.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"real request");
        peer.write_all(b"real response").await.unwrap();
        peer.shutdown().await.unwrap();
        returning.await.unwrap();
        slot_cancel.cancel();
        std::future::poll_fn(|cx| {
            let (progress, fault) = original.poll_cancelled_retirements(cx, Duration::from_secs(3));
            assert!(!progress);
            assert!(fault.is_none());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(!witness.is_joined());
        assert!(adoption.users_closed().is_none());
        assert_eq!(fixture.owner.retained(), 1);
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        finish.send(()).unwrap();
        assert_eq!(scope.next_exit().await.unwrap().id, id);
        std::future::poll_fn(|cx| {
            let (progress, fault) = original.poll_cancelled_retirements(cx, Duration::from_secs(3));
            assert!(fault.is_none());
            if progress {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
        assert!(adoption
            .closed()
            .is_some_and(|receipt| receipt.matches(&adoption) && receipt.had_native_acquisition()));
        assert!(adoption
            .users_closed()
            .is_some_and(|receipt| receipt.matches(&adoption) && receipt.matches_caller(&witness)));
        assert_eq!(fixture.owner.retained(), 0);
        // There has been no second dial or explicit owner shutdown to advance
        // the original. Its real cleanup alone released this same one credit.
        drop(fixture.grants.reserve().unwrap());
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
