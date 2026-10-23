//! Native settlement can finish inside the still-running original caller.
use super::*;

#[tokio::test]
async fn native_settlement_preserves_pending_caller_and_capacity_until_full_actual_join() {
    timeout(Duration::from_secs(5), async {
        for driver in [false, true] {
            let mut fixture = Fixture::new(1).await;
            let registration = fixture.owner.registration();
            let mut caller = TaskScope::new(
                tokio::runtime::Handle::current(),
                NonZeroUsize::new(1).unwrap(),
            );
            let mut transport = TaskScope::new(
                tokio::runtime::Handle::current(),
                NonZeroUsize::new(1).unwrap(),
            );
            let (finish_caller, pending_caller) = tokio::sync::oneshot::channel();
            let (_, caller_witness) = caller
                .registrar()
                .spawn_tracked(TaskClass::Auxiliary, async move {
                    pending_caller.await.unwrap();
                    Ok(())
                })
                .unwrap();
            let mut grant = fixture.grants.reserve().unwrap();
            let user = grant.claim_user_view().unwrap();
            // Library-only attachment of the actual original caller witness;
            // production callers still require the private host issuer.
            assert!(grant
                .slot
                .state()
                .users
                .as_mut()
                .unwrap()
                .bind(caller_witness.clone()));
            let adoption = grant.adoption();
            let carrier = fixture
                .transport
                .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
                .unwrap()
                .open(deadline())
                .await
                .unwrap();
            let (mut peer, _) = fixture.listener.accept().await.unwrap();
            let (mut io, _) = carrier.into_parts();
            io.write_all(b"query").await.unwrap();
            let mut query = [0; 5];
            peer.read_exact(&mut query).await.unwrap();
            assert_eq!(&query, b"query");
            peer.write_all(b"answer").await.unwrap();
            let mut answer = [0; 6];
            io.read_exact(&mut answer).await.unwrap();
            assert_eq!(&answer, b"answer");

            if driver {
                let (returned, view_returned) = tokio::sync::oneshot::channel();
                let (finish, pending) = tokio::sync::oneshot::channel();
                let (id, witness) = adoption
                    .spawn_driver(
                        &transport.registrar(),
                        TaskClass::Auxiliary,
                        io.into_driver(),
                        |io| async move {
                            drop(io);
                            returned.send(()).unwrap();
                            pending.await.unwrap();
                            Ok(())
                        },
                    )
                    .unwrap();
                view_returned.await.unwrap();
                assert!(!witness.is_joined());
                let pending = adoption
                    .retire_native(Instant::now() + Duration::from_millis(10))
                    .await
                    .unwrap();
                assert!(!pending.report().is_complete());
                assert!(pending.closed().is_none());
                assert!(pending
                    .report()
                    .unfinished
                    .contains(&crate::contract::lifecycle::Unfinished::Task(id),));
                assert!(adoption.closed().is_none());
                finish.send(()).unwrap();
                let exit = transport.next_exit().await.unwrap();
                assert_eq!(exit.id, id);
                assert!(witness.is_joined());
            } else {
                // Logical lease/caller is insufficient: original physical I/O
                // itself must return before even native-only confirmation.
                let pending = adoption
                    .retire_native(Instant::now() + Duration::from_millis(10))
                    .await
                    .unwrap();
                assert!(!pending.report().is_complete());
                assert!(pending.closed().is_none());
                assert!(adoption.closed().is_none());
                assert_eq!(
                    peer.try_read(&mut [0; 1]).unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );
                drop(io);
            }
            let native = adoption.retire_native(deadline()).await.unwrap();
            assert!(native.report().is_complete());
            let closed = native.closed().unwrap();
            assert!(closed.matches(&adoption));
            assert!(closed.had_native_acquisition());
            assert_eq!(closed.driver_exit().is_some(), driver);
            assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
            assert!(!caller_witness.is_joined());
            assert!(adoption.users_closed().is_none());
            assert!(registration.closed().is_none());
            assert!(registration.users_closed().is_none());
            assert_eq!(fixture.owner.retained(), 1);
            assert_eq!(
                fixture.grants.reserve().err().unwrap().reason,
                Reason::Capacity
            );
            assert_eq!(
                adoption
                    .slot
                    .state()
                    .pending_close
                    .as_ref()
                    .unwrap()
                    .closed_io
                    .len(),
                1
            );
            assert!(adoption
                .retire_native(deadline())
                .await
                .unwrap()
                .closed()
                .is_some());
            assert!(!adoption
                .retire(Instant::now() + Duration::from_millis(10))
                .await
                .unwrap()
                .is_complete());
            drop(user);
            assert!(!adoption
                .retire(Instant::now() + Duration::from_millis(10))
                .await
                .unwrap()
                .is_complete());
            assert_eq!(
                fixture.grants.reserve().err().unwrap().reason,
                Reason::Capacity
            );
            finish_caller.send(()).unwrap();
            caller.next_exit().await.unwrap();
            assert!(adoption
                .users_closed()
                .unwrap()
                .matches_caller(&caller_witness));
            let full = adoption.retire(deadline()).await.unwrap();
            assert!(full.is_complete());
            assert_eq!(
                full.closed_io.len(),
                1,
                "original full report is consumed once"
            );
            assert!(adoption
                .retire(deadline())
                .await
                .unwrap()
                .closed_io
                .is_empty());
            drop(fixture.grants.reserve().unwrap());
            assert!(fixture
                .owner
                .shutdown(deadline())
                .await
                .unwrap()
                .is_complete());
            assert!(registration.closed().is_some());
            assert!(registration.users_closed().is_some());
            assert!(caller.shutdown(deadline()).await.is_complete());
            assert!(transport.shutdown(deadline()).await.is_complete());
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_only_receipt_does_not_make_default_owner_shutdown_complete_before_caller_join() {
    timeout(Duration::from_secs(4), async {
        let mut fixture = Fixture::new(1).await;
        let registration = fixture.owner.registration();
        let mut caller = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let (finish, pending) = tokio::sync::oneshot::channel();
        let (_, witness) = caller
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                pending.await.unwrap();
                Ok(())
            })
            .unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let user = grant.claim_user_view().unwrap();
        assert!(grant
            .slot
            .state()
            .users
            .as_mut()
            .unwrap()
            .bind(witness.clone()));
        let adoption = grant.adoption();
        let carrier = fixture
            .transport
            .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
            .unwrap()
            .open(deadline())
            .await
            .unwrap();
        let (mut peer, _) = fixture.listener.accept().await.unwrap();
        let (io, _) = carrier.into_parts();
        drop(io);
        let native = adoption.retire_native(deadline()).await.unwrap();
        assert!(native.closed().unwrap().had_native_acquisition());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(!fixture
            .owner
            .shutdown(Instant::now() + Duration::from_millis(10),)
            .await
            .unwrap()
            .is_complete());
        assert!(!witness.is_joined());
        assert!(registration.closed().is_none());
        assert!(registration.users_closed().is_none());
        assert_eq!(fixture.owner.retained(), 1);
        drop(user);
        assert!(!fixture
            .owner
            .shutdown(Instant::now() + Duration::from_millis(10),)
            .await
            .unwrap()
            .is_complete());
        finish.send(()).unwrap();
        caller.next_exit().await.unwrap();
        let report = fixture.owner.shutdown(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        assert!(caller.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
