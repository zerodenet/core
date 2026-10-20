use super::*;

#[tokio::test]
async fn first_host_revocation_is_frozen_and_cancels_prepared_io_while_the_actual_scope_stays_open()
{
    let mut fixture = Fixture::cold(1).await;
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let host = CancellationToken::new();
    let replacement = CancellationToken::new();
    let registration = fixture.owner.registration();
    let description = declaration(&fixture, ControlledStreamDemandClass::Service);
    let binding = registration
        .bind_declared(&description, &scope.registrar(), &host)
        .unwrap();
    let repeated = registration
        .bind_declared(&description, &scope.registrar(), &replacement)
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let grant = fixture.grants.reserve().unwrap();
    let original = grant.adoption();
    let prepared = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
        .unwrap();
    host.cancel();
    assert!(scope.registrar().scope_identity().registration_open());
    assert!(!replacement.is_cancelled());
    assert!(binding.host_cancelled());
    assert!(repeated.host_cancelled());
    let failure = prepared.open(deadline()).await.err().unwrap();
    assert_eq!(failure.reason, Reason::Revoked);
    assert_eq!(failure.delivery, DeliveryState::Unknown);
    assert!(original.closed().is_none());
    assert_eq!(fixture.owner.retained(), 1);
    assert_eq!(
        fixture.grants.reserve().err().unwrap().reason,
        Reason::Revoked
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(registration.closed().unwrap().matches(&registration));
    assert!(
        timeout(Duration::from_millis(20), fixture.listener.accept())
            .await
            .is_err()
    );
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn same_registered_task_observes_host_revocation_without_a_second_scope_or_cancel_task() {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let host = CancellationToken::new();
        let registration = fixture.owner.registration();
        registration
            .bind_declared(
                &declaration(&fixture, ControlledStreamDemandClass::Service),
                &scope.registrar(),
                &host,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let (carrier, mut peer) = fixture.connected().await;
        let (io, adoption) = carrier.into_parts();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (id, witness) = adoption
            .spawn_driver(
                &scope.registrar(),
                TaskClass::Auxiliary,
                io.into_driver(),
                |io| async move {
                    let _original = io;
                    let _ = entered.send(());
                    std::future::pending::<Result<(), ChannelError>>().await
                },
            )
            .unwrap();
        waiting.await.unwrap();
        assert!(!witness.is_joined());
        host.cancel();
        assert!(scope.registrar().scope_identity().registration_open());
        let joined = scope.next_exit().await.unwrap();
        assert_eq!(joined.id, id);
        assert!(witness.wait().await.result().is_ok());
        assert!(registration.closed().is_none());
        assert_eq!(fixture.owner.retained(), 1);
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
