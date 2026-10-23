//! Real caller/transport scopes and one original native slot. These library
//! checks exercise closure facts, not production Engine or host admission.
use super::*;

#[tokio::test]
async fn caller_join_logical_view_return_driver_join_and_native_close_are_distinct_and_cancel_safe()
{
    timeout(Duration::from_secs(4), async {
        for fail_caller in [false, true] {
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
            let (finish, finished) = tokio::sync::oneshot::channel();
            let (_, caller_witness) = caller
                .registrar()
                .spawn_tracked(TaskClass::Critical, async move {
                    finished.await.unwrap();
                    if fail_caller {
                        let mut failure = error(
                            instance(),
                            Stage::Receive,
                            Reason::Io,
                            DeliveryState::Unknown,
                        )
                        .with_source(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "original caller failure",
                        ));
                        failure.session = Some(zero_core::component::SessionId(711));
                        Err(failure)
                    } else {
                        Ok(())
                    }
                })
                .unwrap();
            let mut grant = fixture.grants.reserve().unwrap();
            let user = grant.claim_user_view().unwrap();
            assert!(
                grant.claim_user_view().is_err(),
                "the logical view lease is unique"
            );
            // Model the host-private attachment using the exact actual witness,
            // not a numeric TaskId, completed body or synthetic joined receipt.
            assert!(grant
                .slot
                .state()
                .users
                .as_mut()
                .unwrap()
                .bind(caller_witness.clone()));
            let adoption = grant.adoption();
            assert!(user.matches(&adoption));
            let carrier = fixture
                .transport
                .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
                .unwrap()
                .open(deadline())
                .await
                .unwrap();
            let (mut peer, _) = fixture.listener.accept().await.unwrap();
            let (io, _) = carrier.into_parts();
            let (_, driver_witness) = adoption
                .spawn_driver(
                    &transport.registrar(),
                    TaskClass::Auxiliary,
                    io.into_driver(),
                    |mut io| async move {
                        let mut request = [0; 1];
                        io.read_exact(&mut request).await.unwrap();
                        assert_eq!(&request, b"a");
                        io.write_all(b"b").await.unwrap();
                        Ok(())
                    },
                )
                .unwrap();
            peer.write_all(b"a").await.unwrap();
            let mut byte = [0; 1];
            peer.read_exact(&mut byte).await.unwrap();
            assert_eq!(&byte, b"b");
            transport.next_exit().await.unwrap();
            assert!(driver_witness.is_joined());
            assert!(!caller_witness.is_joined());
            assert!(
                !driver_witness.same_registration(&caller_witness),
                "different scopes can reuse numerical task identities"
            );
            assert!(adoption.closed().is_none());
            assert!(adoption.users_closed().is_none());
            let incomplete = adoption
                .retire(Instant::now() + Duration::from_millis(20))
                .await
                .unwrap();
            assert!(!incomplete.is_complete());
            assert!(
                adoption.closed().is_some(),
                "the original native close is separately observed"
            );
            assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
            assert_eq!(
                fixture.grants.reserve().err().unwrap().reason,
                Reason::Capacity
            );
            assert!(
                adoption.users_closed().is_none(),
                "live logical view still owns its obligation"
            );
            drop(user);
            assert!(!adoption
                .retire(Instant::now() + Duration::from_millis(20))
                .await
                .unwrap()
                .is_complete());
            assert_eq!(
                fixture.grants.reserve().err().unwrap().reason,
                Reason::Capacity
            );
            finish.send(()).unwrap();
            caller.next_exit().await.unwrap();
            let actual = caller_witness.try_joined().unwrap();
            assert_eq!(actual.class(), TaskClass::Critical);
            let users = adoption.users_closed().unwrap();
            assert!(users.had_native_acquisition());
            assert!(adoption.closed().unwrap().had_native_acquisition());
            assert!(users.matches(&adoption));
            assert!(users.matches_caller(&caller_witness));
            assert!(!users.matches_caller(&driver_witness));
            assert_eq!(users.caller_exit().unwrap().id(), actual.id());
            let report = adoption.retire(deadline()).await.unwrap();
            assert!(report.is_complete());
            assert_eq!(
                report.closed_io.len(),
                1,
                "the original close report survives both interrupted waits"
            );
            drop(fixture.grants.reserve().unwrap());
            assert!(
                registration.users_closed().is_none(),
                "only explicit generation shutdown issues a receipt"
            );
            assert!(fixture
                .owner
                .shutdown(deadline())
                .await
                .unwrap()
                .is_complete());
            let generation = registration.users_closed().unwrap();
            assert!(generation.matches(&registration));
            assert!(generation.had_native_acquisition());
            match (actual.result(), generation.first_caller_failure()) {
                (Err(original), Some(first)) => {
                    assert_eq!(first.id(), actual.id());
                    assert_eq!(first.class(), actual.class());
                    let saved = first.result().unwrap_err();
                    assert!(Arc::ptr_eq(&original, &saved));
                    assert_eq!(saved.session, Some(zero_core::component::SessionId(711)));
                    assert_eq!(saved.delivery, DeliveryState::Unknown);
                }
                (Ok(()), None) => {}
                _ => panic!("original caller failure must survive with its exact actual receipt"),
            }
            assert!(caller.shutdown(deadline()).await.is_complete());
            assert!(transport.shutdown(deadline()).await.is_complete());
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn untracked_actual_acquisition_stays_unknown_after_physical_records_are_reclaimed() {
    let mut fixture = Fixture::new(1).await;
    let registration = fixture.owner.registration();
    let (carrier, mut peer) = fixture.connected().await;
    let (_, adoption) = carrier.into_parts();
    assert!(adoption.retire(deadline()).await.unwrap().is_complete());
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    assert_eq!(fixture.owner.retained(), 0);
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(registration.closed().is_some());
    assert!(
        registration.users_closed().is_none(),
        "empty records and native closure cannot invent a caller graph"
    );
}

#[tokio::test]
async fn no_acquisition_user_proof_still_requires_explicit_original_owner_shutdown() {
    let mut fixture = Fixture::new(1).await;
    let registration = fixture.owner.registration();
    assert!(registration.users_closed().is_none());
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    let users = registration.users_closed().unwrap();
    assert!(users.matches(&registration));
    assert!(!users.had_native_acquisition());
    let dropped = Fixture::new(1).await;
    let unconfirmed = dropped.owner.registration();
    drop(dropped);
    assert!(unconfirmed.users_closed().is_none());
}
