use super::*;

#[tokio::test]
async fn global_lookup_custody_retains_original_after_join_until_same_helper_ack() {
    let mut scope = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(3));
    scope.enable_registration().unwrap();
    let registrar = scope.registrar();
    let roots = crate::CarrierOriginalTotals::new(NonZeroUsize::MIN);
    let (mut reader, mut writer) = tokio::io::duplex(1);
    let (ready, pending) = tokio::sync::oneshot::channel();
    let (caller_id, caller) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            let read = reader.read_u8();
            tokio::pin!(read);
            let mut ready = Some(ready);
            poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(ready) = ready.take() {
                        ready.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .map_err(|source| error(Stage::Receive, Reason::Io).with_source(source))?;
            Ok(())
        })
        .unwrap();
    pending.await.unwrap();
    let prepare = || {
        let original = LocalProcessLookup::prepare(
            caller.clone(),
            "127.0.0.1:1".parse().unwrap(),
            LocalProcessTransport::Tcp,
            registrar.cancellation(),
            NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        original
            .bind_original_totals(instance(), &registrar.downgrade(), &roots)
            .unwrap();
        original
    };
    let original = prepare();
    assert_eq!(roots.snapshot().charged, 0);
    let start = original.register(&registrar.downgrade()).unwrap();
    // This actual worker is blocked on its own pre-IO Condvar, not a marker.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !original.state.lock().unwrap().waiting_at_start {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(roots.snapshot().charged, 1);
    let denied = prepare();
    let source = denied.register(&registrar.downgrade()).err().unwrap();
    assert_eq!(source.reason, Reason::Capacity);
    assert_eq!(source.delivery, DeliveryState::NotSubmitted);
    assert!(denied.witness().is_none());
    assert!(
        denied.retired(),
        "known no-task/no-native failure creates no retained debt"
    );
    assert_eq!(roots.snapshot().charged, 1);
    let actual = original.witness().unwrap();
    let held = Arc::downgrade(&original);
    drop(start); // This same worker now exits without acquiring any OS object.
    drop(original);
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), actual.task_id().unwrap());
    actual.wait().await.result().unwrap();
    assert!(!actual.try_joined().unwrap().registration_released());
    assert!(
        held.upgrade().is_some(),
        "the same root strongly retains the original after its real join"
    );
    assert_eq!(roots.snapshot().charged, 1);
    assert!(!caller.is_joined());
    scope.acknowledge(&receipt).unwrap();
    assert_eq!(roots.snapshot().charged, 0);
    assert!(
        held.upgrade().is_none(),
        "no self/root strong cycle remains after exact settlement"
    );
    let next = prepare();
    let start = next.register(&registrar.downgrade()).unwrap();
    assert_eq!(roots.snapshot().charged, 1);
    drop(start);
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), next.witness().unwrap().task_id().unwrap());
    scope.acknowledge(&receipt).unwrap();
    assert_eq!(roots.snapshot().charged, 0);
    assert!(
        !caller.is_joined(),
        "same caller and Scope remain usable after only metadata retirement"
    );
    writer.write_u8(1).await.unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), caller_id);
    scope.acknowledge(&receipt).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
}
