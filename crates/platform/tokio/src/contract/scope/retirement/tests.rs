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

#[tokio::test]
async fn paid_completion_survives_scope_retirement_without_retaining_its_tasks_or_faking_drop_ack()
{
    let root = std::sync::Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let mut scope = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    let registrar = scope.registrar();
    let weak = registrar.downgrade();
    let completion = weak.retirement_completion(&memory).unwrap();
    assert!(!completion.is_drained());
    scope.enable_registration().unwrap();
    let (finish, finished) = oneshot::channel();
    let (_, witness) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            finished.await.unwrap();
            Ok(())
        })
        .unwrap();
    scope.begin_shutdown();
    finish.send(()).unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert!(witness.matches_joined(receipt.joined()));
    assert!(!completion.is_drained());
    let callbacks = scope.defer_callbacks();
    scope.acknowledge(&receipt).unwrap();
    assert!(!completion.is_drained());
    drop(callbacks);
    assert!(witness.try_joined().unwrap().registration_released());
    assert!(completion.is_drained());
    drop(registrar);
    drop(receipt);
    drop(scope);
    assert!(weak.retirement_observation().is_none());
    assert!(completion.is_drained());
    let other = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    let abandoned = other
        .registrar()
        .downgrade()
        .retirement_completion(&memory)
        .unwrap();
    let mut other = other;
    other.enable_registration().unwrap();
    let (_, pending) = oneshot::channel::<()>();
    other
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let _ = pending.await;
            Ok(())
        })
        .unwrap();
    drop(other);
    assert!(
        !abandoned.is_drained(),
        "abort/Drop is not a native task join or ACK"
    );
}
