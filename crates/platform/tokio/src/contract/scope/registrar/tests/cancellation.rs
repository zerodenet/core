use super::*;

#[tokio::test]
async fn weak_cancellation_preserves_original_one_way_scope_stop_without_retaining_its_owner() {
    let mut scope = TaskScope::suspended_hosted(Handle::current(), None);
    let weak = scope.registrar().downgrade();
    let identity = weak.scope_identity();
    let local = weak.cancellation().unwrap();
    let sibling = scope.registrar().cancellation();
    assert_eq!(weak.0.strong_count(), 1);
    assert!(
        !identity.resource_open(),
        "observing cancellation cannot open build admission"
    );
    assert!(!local.is_cancelled());
    assert!(!sibling.is_cancelled());
    local.cancel();
    assert!(local.is_cancelled());
    assert!(
        !sibling.is_cancelled(),
        "weak caller cannot cancel its Scope or another child"
    );
    let observed = weak.cancellation().unwrap();
    assert!(!observed.is_cancelled());
    scope.enable_registration().unwrap();
    scope.begin_shutdown();
    tokio::time::timeout(Duration::from_secs(1), observed.cancelled())
        .await
        .unwrap();
    assert!(
        sibling.is_cancelled(),
        "real original Scope cancellation reaches both APIs"
    );
    assert!(weak.cancellation().unwrap().is_cancelled());
    drop(scope);
    assert!(
        weak.0.upgrade().is_none(),
        "returned tokens retain no Scope/runtime/task owner"
    );
    assert!(weak.cancellation().is_none());
}

#[tokio::test]
async fn actual_scope_stop_wakes_the_same_weak_registered_pending_task_and_only_real_join_confirms_it(
) {
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let weak = scope.registrar().downgrade();
    let stop = weak.cancellation().unwrap();
    let (mut io, _remote) = tokio::io::duplex(1);
    let (entered, pending) = oneshot::channel();
    let (id, witness) = weak.spawn_tracked(TaskClass::Auxiliary, async move {
        let mut entered = Some(entered);
        let read = io.read_u8();
        tokio::pin!(read);
        tokio::select! {
            _ = stop.cancelled() => Ok(()),
            result = poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() { entered.send(()).unwrap(); }
                }
                result
            }) => panic!("original I/O should remain Pending until actual Scope stop: {result:?}"),
        }
    }).unwrap();
    pending.await.unwrap();
    assert!(!witness.is_joined());
    assert_eq!(weak.0.strong_count(), 1);
    scope.begin_shutdown();
    let exit = tokio::time::timeout(Duration::from_secs(1), scope.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exit.id, id);
    assert_eq!(exit.class, TaskClass::Auxiliary);
    exit.result.unwrap();
    assert_eq!(witness.wait().await.id(), id);
    assert!(scope.next_exit().await.is_none());
}
