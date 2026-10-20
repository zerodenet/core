use super::*;
#[tokio::test]
async fn same_slot_auxiliary_uses_original_scope_and_cleanup_accepts_the_real_failure_receipt() {
    timeout(Duration::from_secs(4), async {
        let mut fixture = Fixture::cold(1).await;
        let mut actual = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let mut foreign = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let host = Arc::new(CancellationToken::new());
        fixture
            .owner
            .registration()
            .bind_declared(
                &crate::contract::component::ResourceDeclaration::ControlledOutboundStream {
                    endpoint: fixture.destination.clone(),
                    binding: fixture.transport.binding().clone(),
                    demand: crate::contract::component::ControlledStreamDemandClass::Service,
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &actual.registrar(),
                &host,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let (carrier, mut peer) = fixture.connected().await;
        let (io, adoption) = carrier.into_parts();
        assert_eq!(
            adoption
                .spawn_auxiliary(&foreign.registrar(), async { Ok(()) })
                .unwrap_err()
                .reason,
            Reason::Revoked
        );
        let (finish, finished) = tokio::sync::oneshot::channel();
        let (id, witness) = adoption
            .spawn_auxiliary(&actual.registrar(), async move {
                finished.await.unwrap();
                Err(error(
                    instance(),
                    Stage::Receive,
                    Reason::Io,
                    DeliveryState::Unknown,
                )
                .with_source(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "same original refresh failure",
                )))
            })
            .unwrap();
        assert!(adoption
            .spawn_auxiliary(&actual.registrar(), async { Ok(()) })
            .is_err());
        assert!(adoption.auxiliary_exit().is_none());
        drop(io);
        let retirement = adoption
            .retire(Instant::now() + Duration::from_millis(10))
            .await
            .unwrap();
        assert!(!retirement.is_complete());
        assert!(adoption.closed().is_none());
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        finish.send(()).unwrap();
        let exit = actual.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        let original = witness.try_joined().unwrap().result().unwrap_err();
        // No request-side local receiver consumes it; persistent cleanup does.
        assert!(adoption.auxiliary_exit().is_none());
        assert!(adoption.retire(deadline()).await.unwrap().is_complete());
        let retained = adoption.auxiliary_exit().unwrap();
        assert_eq!(retained.id(), id);
        assert_eq!(retained.class(), TaskClass::Auxiliary);
        assert!(Arc::ptr_eq(&retained.result().unwrap_err(), &original));
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(actual.shutdown(deadline()).await.is_complete());
        assert!(foreign.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
