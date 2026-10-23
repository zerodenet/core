use super::*;
#[tokio::test]
async fn inline_reap_never_cancels_a_live_caller_and_closes_the_original_fd_before_reusing_capacity(
) {
    let mut fixture = Fixture::new(1).await;
    let mut caller = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let (finish, finished) = tokio::sync::oneshot::channel();
    let (_, witness) = caller
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            finished.await.unwrap();
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
    assert!(fixture.grants.registration().matches_adoption(&adoption));
    let carrier = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
        .unwrap()
        .open(deadline())
        .await
        .unwrap();
    let (mut peer, _) = fixture.listener.accept().await.unwrap();
    let (mut io, _) = carrier.into_parts();
    io.write_all(b"a").await.unwrap();
    let mut byte = [0; 1];
    peer.read_exact(&mut byte).await.unwrap();
    assert_eq!(&byte, b"a");
    assert_eq!(fixture.grants.reap_returned(deadline()).await.unwrap(), 0);
    assert!(!witness.is_joined());
    drop(io);
    drop(user);
    assert_eq!(fixture.grants.reap_returned(deadline()).await.unwrap(), 0);
    assert!(adoption.closed().is_none());
    assert_eq!(
        peer.try_read(&mut byte).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        fixture.grants.reserve().err().unwrap().reason,
        Reason::Capacity
    );
    finish.send(()).unwrap();
    caller.next_exit().await.unwrap();
    assert_eq!(fixture.grants.reap_returned(deadline()).await.unwrap(), 1);
    assert!(adoption.closed().unwrap().had_native_acquisition());
    assert!(adoption.users_closed().unwrap().matches_caller(&witness));
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    assert_eq!(fixture.grants.reap_returned(deadline()).await.unwrap(), 0);
    drop(fixture.grants.reserve().unwrap());
    assert!(
        fixture.owner.registration().closed().is_none(),
        "reap is not generation shutdown"
    );
    let report = fixture.owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io.len(),
        1,
        "inline maintenance retains the actual original close report for lifecycle"
    );
    assert!(caller.shutdown(deadline()).await.is_complete());
}
