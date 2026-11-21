use super::*;
use std::future::Future;

#[tokio::test]
async fn related_pair_waits_for_original_close_without_holding_half_or_revoking_live_io() {
    let mut fixture = Fixture::new(2).await;
    let (carrier, mut peer) = fixture.connected().await;
    let (mut io, original) = carrier.into_parts();
    let spare = fixture.grants.reserve().unwrap();
    let mut pending = Box::pin(fixture.grants.reserve_pair(deadline()));
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(spare);
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(fixture.owner.retained(), 1, "no half-pair reservation");
    assert!(original.closed().is_none());
    let mut bytes = [0; 4];
    let mut read = tokio::io::ReadBuf::new(&mut bytes);
    std::future::poll_fn(|cx| {
        assert!(
            tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut io), cx, &mut read)
                .is_pending()
        );
        std::task::Poll::Ready(())
    })
    .await;
    peer.write_all(b"live").await.unwrap();
    io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"live");
    drop(io);
    assert!(original.retire(deadline()).await.unwrap().is_complete());
    assert!(original.closed().unwrap().matches(&original));
    let pair = pending.await.unwrap();
    assert_eq!(fixture.owner.retained(), 2);
    drop(pair);
    assert_eq!(fixture.owner.retained(), 0);

    let impossible = Fixture::new(1).await;
    assert_eq!(
        impossible
            .grants
            .reserve_pair(deadline())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Capacity
    );
    assert_eq!(impossible.owner.retained(), 0);
    let held = fixture.grants.reserve_pair(deadline()).await.unwrap();
    assert_eq!(
        fixture
            .grants
            .reserve_pair(Instant::now())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Timeout
    );
    assert_eq!(fixture.owner.retained(), 2);
    drop(held);
    let mut pending = Box::pin(fixture.grants.reserve_pair(deadline()));
    fixture.owner.revoke();
    assert_eq!(
        pending.as_mut().await.err().unwrap().reason,
        Reason::Revoked
    );
    assert_eq!(fixture.owner.retained(), 0);
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
}

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
