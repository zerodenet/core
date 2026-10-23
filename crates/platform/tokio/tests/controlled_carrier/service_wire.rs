use super::service::description;
use super::*;
use crate::contract::{component::ControlledStreamDemandClass, network::UpstreamOpener};

#[tokio::test]
async fn actual_service_query_owns_bare_tcp_and_native_close_never_joins_its_current_caller() {
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
                &description(&fixture, ControlledStreamDemandClass::Service),
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
            .service_issuer(&original, &grant, witness.clone())
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
