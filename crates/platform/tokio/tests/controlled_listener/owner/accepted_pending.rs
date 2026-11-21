use super::*;
use crate::contract::scope::TaskScope;

#[tokio::test]
async fn listener_shutdown_waits_for_the_cancelled_native_accept_to_return_its_reservation() {
    let one = NonZeroUsize::new(1).unwrap();
    let address = "127.0.0.1:0".parse().unwrap();
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), one);
    scope.enable_registration().unwrap();
    let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
        &declaration(address, 1),
        one,
        CancellationToken::new(),
        one,
        one,
        &scope.registrar(),
    )
    .unwrap();
    let mut listener = original.bind_listener(address).unwrap();
    let mut accept = Box::pin(listener.accept_owned());
    poll_fn(|cx| {
        assert!(accept.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(original.accepted_evidence().unwrap().retained, 1);

    let end = deadline();
    let mut closing = Box::pin(original.wait_closed(end));
    poll_fn(|cx| {
        assert!(
            closing.as_mut().poll(cx).is_pending(),
            "the original accept still owns its no-native reservation"
        );
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert_eq!(original.confirmed_close_count(), 1);
    assert_eq!(original.accepted_evidence().unwrap().retained, 1);

    let mut closing = Box::pin(original.wait_closed(end));
    poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let error = accept.await.err().expect("same accept must be revoked");
    assert_eq!(error.reason, Reason::Revoked);
    let report = closing.await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.closed_io.len(), 1, "the original listener receipt");
    assert_eq!(original.confirmed_close_count(), 1);
    assert_eq!(original.accepted_evidence().unwrap().retained, 0);
    let joined = scope.shutdown(end).await;
    assert!(joined.is_complete());
    assert!(joined.exited_tasks.is_empty());
}
