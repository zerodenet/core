use super::*;
#[tokio::test]
async fn multi_candidate_request_default_denies_without_legacy_execution_or_scope_owner_retention()
{
    use std::{future::Future, task::Poll};
    let mut fixture = Fixture::new(1).await;
    let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), None);
    scope.enable_registration().unwrap();
    let weak = scope.registrar().downgrade();
    let request = crate::contract::network::ControlledStreamRequest {
        destination: fixture.destination.clone(),
        context: CarrierContext {
            session: Some(zero_core::component::SessionId(88)),
            deadline: None,
            cancel: CancellationToken::new(),
        },
        grants: fixture.grants.clone(),
        tasks: weak.clone(),
        retirement: Duration::from_secs(1),
    };
    let failure = fixture
        .transport
        .open_controlled_stream(request)
        .await
        .err()
        .unwrap();
    assert_eq!(failure.component, Some(instance().component));
    assert_eq!(failure.session, Some(zero_core::component::SessionId(88)));
    assert_eq!(failure.stage, Stage::Prepare);
    assert_eq!(failure.reason, Reason::Unsupported);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert_eq!(fixture.owner.retained(), 0);
    {
        let accepting = fixture.listener.accept();
        tokio::pin!(accepting);
        std::future::poll_fn(|cx| {
            assert!(accepting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    // Denial never reroutes through this existing real single-carrier opener.
    // It remains independently usable with its original Service semantics.
    let (carrier, mut peer) = fixture.connected().await;
    let (mut io, adoption) = carrier.into_parts();
    io.write_all(b"x").await.unwrap();
    let mut byte = [0; 1];
    peer.read_exact(&mut byte).await.unwrap();
    assert_eq!(&byte, b"x");
    drop(io);
    assert!(adoption.retire(deadline()).await.unwrap().is_complete());
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
    drop(scope);
    assert!(
        weak.defer_callbacks().is_none(),
        "request did not retain an original Scope owner"
    );
}
