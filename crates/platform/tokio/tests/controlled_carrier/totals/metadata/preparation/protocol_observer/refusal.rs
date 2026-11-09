use super::*;
pub(super) async fn known_unregistered_original_returns_while_same_scope_real_task_remains_pending(
    scope: &mut TaskScope,
    tasks: &crate::contract::scope::TaskRegistrar,
    owner: &MetadataPoolOwner,
    totals: &Arc<CarrierOriginalTotals>,
    upstream: &UpstreamTransport,
    foreign: &UpstreamTransport,
    endpoint: &zero_core::request::Destination,
) {
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    let (_, blocker) = tasks
        .spawn_tracked(crate::contract::scope::TaskClass::Auxiliary, async move {
            let _ = held.await;
            Ok(())
        })
        .unwrap();
    assert!(!blocker.is_joined());
    let closing = Arc::new(OriginalClose {
        ready: AtomicBool::new(true),
        changed: tokio::sync::Notify::new(),
        origin: OnceLock::new(),
        tasks: tasks.clone(),
        wire: Mutex::new(None),
        claim: Mutex::new(None),
    });
    let (prepared, retirement) = owner
        .grants()
        .prepare_protocol_original(tasks, Box::new(Control(closing.clone())).into())
        .unwrap_or_else(|error| panic!("{:?}", error.error));
    let original = prepared.origin();
    assert!(closing.origin.set(original.clone()).is_ok());
    let token = prepared
        .protocol_original(
            upstream,
            upstream.protocol_original_observer().unwrap(),
            endpoint,
            &retirement,
        )
        .unwrap();
    assert!(foreign
        .protocol_original_observer()
        .unwrap()
        .retain_protocol_original(token.clone())
        .is_err());
    assert!(original.initializer_registration().is_none());
    assert!(original.endpoints().is_none());
    // This is a known pre-registrar refusal, not an uncertain spawn unwind.
    // Drop publishes preparing=false then wakes the same persistent Root phase.
    drop(prepared);
    token.request_retirement();
    std::future::poll_fn(|cx| {
        totals.poll_cancelled_retirements(cx);
        if totals.snapshot().charged == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    assert!(!blocker.is_joined());
    assert!(!scope.is_drained());
    assert!(token.is_settled());
    let never_native = original.closed().unwrap();
    assert!(never_native.matches(&original) && !never_native.had_native_acquisition());
    // Same actual local N1 and Root N1 are usable again before the unrelated
    // real task returns; neither quota nor timeout is relaxed.
    let again = owner
        .grants()
        .prepare_original(tasks)
        .expect("same N1 returned");
    assert_eq!(totals.snapshot().charged, 1);
    let next = again.origin();
    assert!(!next.same_original(&original));
    drop(again);
    std::future::poll_fn(|cx| {
        totals.poll_cancelled_retirements(cx);
        if totals.snapshot().charged == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    assert!(!blocker.is_joined());
    assert!(!next.closed().unwrap().had_native_acquisition());
    release.send(()).unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert!(blocker.matches_joined(receipt.joined()) && receipt.result().is_ok());
    scope.acknowledge(&receipt).unwrap();
    assert!(blocker
        .wait_registration_released()
        .await
        .registration_released());
}
