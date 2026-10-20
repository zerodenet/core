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
        let original_slot = Arc::downgrade(&adoption.slot);
        let (id, witness) = adoption
            .spawn_auxiliary(&actual.registrar(), async move {
                let installed = original_slot.upgrade().unwrap();
                assert!(
                    installed.state().auxiliary.is_some(),
                    "real work cannot poll before its original task witness is installed"
                );
                drop(installed);
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

#[tokio::test]
async fn refused_auxiliary_future_drop_reenters_original_slot_outside_both_reservation_locks() {
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll},
    };
    struct Reenter {
        adoption: CarrierAdoption,
        dropped: Arc<AtomicBool>,
    }
    impl Future for Reenter {
        type Output = Result<(), crate::contract::lifecycle::ComponentError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            panic!("the capacity-refused future must never be polled")
        }
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            assert!(
                self.adoption.slot.closing.try_lock().is_ok(),
                "original retirement mutex is not held across future destruction"
            );
            assert!(
                self.adoption.slot.state.try_lock().is_ok(),
                "original state mutex is not held across future destruction"
            );
            assert!(self.adoption.endpoints().is_some());
            self.dropped.store(true, Ordering::Release);
        }
    }
    let mut fixture = Fixture::cold(1).await;
    let mut scope = TaskScope::new(
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
            &scope.registrar(),
            &host,
        )
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    let (carrier, mut peer) = fixture.connected().await;
    let (io, adoption) = carrier.into_parts();
    let (finish, finished) = tokio::sync::oneshot::channel();
    scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            finished.await.unwrap();
            Ok(())
        })
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let error = adoption
        .spawn_auxiliary(
            &scope.registrar(),
            Reenter {
                adoption: adoption.clone(),
                dropped: dropped.clone(),
            },
        )
        .unwrap_err();
    assert_eq!(error.reason, Reason::Capacity);
    assert!(dropped.load(Ordering::Acquire));
    assert!(adoption.auxiliary_exit().is_none());
    drop(io);
    assert!(adoption.retire(deadline()).await.unwrap().is_complete());
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    finish.send(()).unwrap();
    scope.next_exit().await.unwrap();
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
