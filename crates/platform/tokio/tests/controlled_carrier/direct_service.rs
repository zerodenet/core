use super::*;
use crate::contract::network::UpstreamOpener;

#[tokio::test]
async fn actual_direct_service_uses_one_original_owner_and_retains_credit_until_its_real_caller_join(
) {
    timeout(Duration::from_secs(5), async {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let original = Arc::new(CancellationToken::new());
        let bound = fixture
            .owner
            .registration()
            .bind_declared(
                &crate::contract::component::ResourceDeclaration::ControlledDirectStream {
                    dial: fixture.transport.binding().dial.clone(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &original,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let (ready, preparing) = tokio::sync::oneshot::channel::<(
            NativeCarrierOpener,
            UpstreamBinding,
            Destination,
            CarrierGrant,
            CarrierUserView,
            CarrierContext,
        )>();
        let (observed, observing) = tokio::sync::oneshot::channel();
        let (release, releasing) = tokio::sync::oneshot::channel();
        let (id, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let (native, binding, endpoint, grant, user, context) = preparing.await.unwrap();
                let connected = native
                    .prepare_controlled_stream(&binding, endpoint, context, grant)
                    .unwrap()
                    .open(deadline())
                    .await
                    .unwrap();
                let (mut io, adoption) = connected.into_parts();
                io.write_all(b"root service request").await.unwrap();
                io.shutdown().await.unwrap();
                let mut reply = Vec::new();
                io.read_to_end(&mut reply).await.unwrap();
                assert_eq!(reply, b"reply after actual FIN");
                drop((io, user));
                let native = adoption.retire_native(deadline()).await.unwrap();
                assert!(native.closed().is_some());
                observed
                    .send(native)
                    .unwrap_or_else(|_| panic!("original native settlement receiver"));
                releasing.await.unwrap();
                Ok(())
            })
            .unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let adoption = grant.adoption();
        let user = grant.claim_user_view().unwrap();
        let context = context();
        let cancel = context.cancel.clone();
        let ticket = bound
            .direct_service_issuer(
                &original,
                &grant,
                witness.clone(),
                &fixture.destination,
                fixture.transport.binding(),
            )
            .unwrap()
            .authorize(cancel.clone(), context.deadline)
            .unwrap();
        let native = NativeCarrierOpener::service(ticket, fixture.egress.clone(), None).unwrap();
        ready
            .send((
                native,
                fixture.transport.binding().clone(),
                fixture.destination.clone(),
                grant,
                user,
                context,
            ))
            .unwrap_or_else(|_| panic!("original query receiver"));
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let mut request = Vec::new();
        peer.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"root service request");
        peer.write_all(b"reply after actual FIN").await.unwrap();
        peer.shutdown().await.unwrap();
        let native = observing.await.unwrap();
        let closed = native.closed().unwrap();
        assert!(closed.matches(&adoption));
        assert!(closed.had_native_acquisition());
        assert!(closed.driver_exit().is_none());
        assert!(!witness.is_joined());
        assert!(
            !cancel.is_cancelled(),
            "native control cancellation cannot cancel the query parent"
        );
        assert!(adoption.users_closed().is_none());
        assert_eq!(fixture.owner.retained(), 1);
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        release.send(()).unwrap();
        let actual = scope.next_exit().await.unwrap();
        assert_eq!(actual.id, id);
        assert_eq!(witness.try_joined().unwrap().id(), id);
        assert!(adoption.users_closed().unwrap().matches_caller(&witness));
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
#[tokio::test]
async fn direct_service_still_requires_original_fence_scope_literal_and_an_explicit_ticket() {
    let mut fixture = Fixture::cold(1).await;
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut foreign = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let original = Arc::new(CancellationToken::new());
    let registration = fixture.owner.registration();
    let bound = registration
        .bind_declared(
            &crate::contract::component::ResourceDeclaration::ControlledDirectStream {
                dial: fixture.transport.binding().dial.clone(),
                max_sockets: NonZeroUsize::new(1).unwrap(),
            },
            &scope.registrar(),
            &original,
        )
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    // Same original descriptor and grantee still cannot open a bare Service.
    let grant = fixture.grants.reserve().unwrap();
    let adoption = grant.adoption();
    let native = NativeCarrierOpener::new(
        instance(),
        fixture.transport.binding().clone(),
        fixture.listener.local_addr().unwrap(),
        fixture.egress.clone(),
        None,
    )
    .unwrap();
    assert!(native
        .prepare_controlled_stream(
            fixture.transport.binding(),
            fixture.destination.clone(),
            context(),
            grant
        )
        .is_err());
    assert!(!adoption.closed().unwrap().had_native_acquisition());
    let (done, waiting) = tokio::sync::oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            waiting.await.unwrap();
            Ok(())
        })
        .unwrap();
    let (other_done, other_waiting) = tokio::sync::oneshot::channel();
    let (_, other) = foreign
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            other_waiting.await.unwrap();
            Ok(())
        })
        .unwrap();
    let mut grant = fixture.grants.reserve().unwrap();
    let user = grant.claim_user_view().unwrap();
    assert!(bound
        .direct_service_issuer(
            &Arc::new(original.as_ref().clone()),
            &grant,
            witness.clone(),
            &fixture.destination,
            fixture.transport.binding()
        )
        .is_err());
    assert!(bound
        .direct_service_issuer(
            &original,
            &grant,
            other,
            &fixture.destination,
            fixture.transport.binding()
        )
        .is_err());
    let mut prefix = fixture.transport.binding().clone();
    prefix.prefix.push(zero_core::component::RoleInstanceId {
        component: ComponentId(999),
        generation: ResourceGeneration(1),
        role: zero_core::component::RoleId::Outbound(zero_core::component::OutboundId(999)),
    });
    assert!(bound
        .direct_service_issuer(
            &original,
            &grant,
            witness.clone(),
            &fixture.destination,
            &prefix
        )
        .is_err());
    let domain = Destination::new(
        Address::Domain("foreign.invalid".into()),
        fixture.destination.port,
    );
    assert!(bound
        .direct_service_issuer(
            &original,
            &grant,
            witness.clone(),
            &domain,
            fixture.transport.binding()
        )
        .is_err());
    assert!(grant.slot.state().control.is_none());
    assert!(grant.slot.state().unknown.is_none());
    done.send(()).unwrap();
    scope.next_exit().await.unwrap();
    assert!(bound
        .direct_service_issuer(
            &original,
            &grant,
            witness,
            &fixture.destination,
            fixture.transport.binding()
        )
        .is_err());
    other_done.send(()).unwrap();
    foreign.next_exit().await.unwrap();
    drop((grant, user));
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
    assert!(foreign.shutdown(deadline()).await.is_complete());
}

#[path = "direct_service/preparing.rs"]
mod preparing;

#[path = "direct_service/connect_rejection.rs"]
mod connect_rejection;
