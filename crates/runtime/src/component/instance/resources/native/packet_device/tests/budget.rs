use super::*;
#[tokio::test]
async fn original_read_capacity_is_rejected_before_native_acquisition_and_retains_real_driver() {
    let mut actual = scope();
    let roots = Arc::new(crate::HostOriginalBudgets::default());
    roots.packet.install_native(1, 65_535).unwrap();
    let original = owner_with_budget(&actual, i32::MAX, "not-a-tun", roots.clone());
    let (send, pending, witness) = driver(&actual);
    pending.await.unwrap();
    let failure = failed(original.open(witness.clone()).await);
    assert_eq!(failure.reason, Reason::Capacity);
    assert!(matches!(
        original.original.outcome(),
        TunAcquisitionOutcome::KnownNoNative
    ));
    assert!(!witness.is_joined());
    assert_eq!(roots.packet.native().occupied.frames, 0);
    let mut closing = Box::pin(original.shutdown(deadline()));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert!(!original.evidence().physical_confirmed());
    drop(send);
    joined(&mut actual, &witness).await;
    let report = original.shutdown(deadline()).await;
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
    assert!(original.evidence().physical_confirmed());
    actual.begin_shutdown();
    assert!(actual.next_receipt().await.is_none());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn inline_original_device_read_set_stays_stopped_and_returns_only_actual_memory_after_last_buffer(
) {
    let mut actual = scope();
    let roots = Arc::new(crate::HostOriginalBudgets::default());
    roots.packet.install_native(1, 65_536).unwrap();
    let original = owner_with_budget(&actual, i32::MAX, "not-a-tun", roots.clone());
    let baseline = roots.packet.driver_metadata().occupied;
    let read = original
        .memory
        .prepare(&original.original, ComponentId(7))
        .unwrap();
    let held = read.try_buffer().unwrap();
    drop(read);
    assert!(matches!(
        original.original.outcome(),
        TunAcquisitionOutcome::KnownNoNative
    ));
    original.memory.stop(&original.original);
    assert_eq!(roots.packet.native().occupied.frames, 1);
    assert!(roots.packet.driver_metadata().occupied.bytes() > baseline.bytes());
    let failure = original
        .memory
        .prepare(&original.original, ComponentId(7))
        .err()
        .unwrap();
    assert_eq!(
        failure.reason,
        Reason::Revoked,
        "detached read set is never permission to prepare a second epoch"
    );
    assert!(
        !original.evidence().physical_confirmed(),
        "memory return is not Native/driver/Scope close"
    );
    drop(held);
    assert_eq!(roots.packet.native().occupied.frames, 0);
    assert_eq!(roots.packet.driver_metadata().occupied, baseline);
    actual.begin_shutdown();
    assert!(actual.next_receipt().await.is_none());
}
