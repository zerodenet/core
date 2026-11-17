use super::*;

#[tokio::test]
async fn inline_reaping_keeps_the_actual_reserved_grant_until_native_preparation() {
    let mut fixture = Fixture::new(1).await;
    let mut grant = fixture.grants.reserve().unwrap();
    let original = grant.adoption();
    assert_eq!(fixture.owner.retained(), 1);
    assert!(
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(fixture.listener.poll_accept(cx).is_pending())
        })
        .await
    );
    // Another real caller runs the production inline reaper while this exact
    // reserved capability has not installed its user or performed native IO.
    assert_eq!(fixture.grants.reap_returned(deadline()).await.unwrap(), 0);
    assert!(original.closed().is_none());
    assert_eq!(
        fixture.grants.reserve().err().unwrap().reason,
        Reason::Capacity
    );
    let user = grant.claim_user_view().unwrap();
    let mut caller = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let (finish, finished) = tokio::sync::oneshot::channel();
    let (_, witness) = caller
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            finished.await.unwrap();
            Ok(())
        })
        .unwrap();
    assert!(grant
        .slot
        .state()
        .users
        .as_mut()
        .unwrap()
        .bind(witness.clone()));
    let carrier = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
        .unwrap()
        .open(deadline())
        .await
        .unwrap();
    let (mut peer, _) = fixture.listener.accept().await.unwrap();
    let (mut io, actual) = carrier.into_parts();
    assert!(user.matches(&actual));
    io.write_all(b"same-original").await.unwrap();
    let mut bytes = [0; 13];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"same-original");
    drop(io);
    drop(user);
    finish.send(()).unwrap();
    caller.next_exit().await.unwrap();
    assert!(witness.is_joined());
    let report = actual.retire(deadline()).await.unwrap();
    assert!(report.is_complete());
    let receipt = actual.closed().unwrap();
    assert!(receipt.matches(&actual));
    assert!(receipt.had_native_acquisition());
    assert_eq!(peer.read(&mut bytes).await.unwrap(), 0);
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
}
