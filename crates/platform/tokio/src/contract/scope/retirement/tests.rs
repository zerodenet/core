use super::*;
use tokio::sync::oneshot;

#[tokio::test]
async fn same_scope_observation_waits_for_actual_join_and_deferred_host_ack() {
    let mut scope = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    let registrar = scope.registrar();
    let observation = registrar.retirement_observation();
    assert!(observation.scope_identity().matches(&registrar));
    assert!(!observation.is_drained(), "cold empty census is not sealed");
    scope.enable_registration().unwrap();
    let (finish, finished) = oneshot::channel();
    let (_, witness) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            finished.await.unwrap();
            Ok(())
        })
        .unwrap();
    scope.begin_shutdown();
    assert!(!observation.is_drained());
    finish.send(()).unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert!(witness.matches_joined(receipt.joined()));
    assert!(
        !observation.is_drained(),
        "actual joined exit still awaits Host ACK"
    );
    let waiting = observation.wait_drained();
    tokio::pin!(waiting);
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let callbacks = scope.defer_callbacks();
    scope.acknowledge(&receipt).unwrap();
    assert!(!observation.is_drained());
    drop(callbacks);
    assert!(observation.is_drained());
    waiting.await;
    assert!(scope.next_receipt().await.is_none());
}

#[tokio::test]
async fn dropping_host_does_not_turn_an_unconsumed_original_registration_into_retirement() {
    let mut scope = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let observation = scope.registrar().retirement_observation();
    let (finish, finished) = oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            finished.await.unwrap();
            Ok(())
        })
        .unwrap();
    drop(scope);
    finish.send(()).unwrap();
    assert!(!witness.is_joined());
    assert!(
        !observation.is_drained(),
        "Drop is no join or publication ACK"
    );
}
