use super::*;

#[cfg(target_os = "macos")]
#[tokio::test]
async fn same_original_macos_cleanup_memory_survives_wire_stop_until_provider_close() {
    let mut actual = scope();
    let roots = Arc::new(crate::HostOriginalBudgets::default());
    let original = owner_with_budget(&actual, i32::MAX, "not-a-tun", roots.clone());
    let read = original
        .memory
        .prepare(&original.original, ComponentId(7))
        .unwrap();
    let memory = read.memory_view().unwrap();
    original.memory.stop(&original.original);
    assert_eq!(
        original
            .memory
            .prepare(&original.original, ComponentId(7))
            .err()
            .unwrap()
            .reason,
        Reason::Revoked
    );
    let credit = memory
        .reserve_driver_metadata(zero_tun::packet_memory::PacketMetadataLayout::new(1, 16))
        .expect("original helper cleanup retains its actual memory admission");
    drop(credit);
    assert!(!original.evidence().physical_confirmed());
    assert!(matches!(
        original.original.outcome(),
        TunAcquisitionOutcome::KnownNoNative
    ));
    let report = original.shutdown(deadline()).await;
    assert!(report.is_complete());
    assert!(
        report.closed_io.is_empty(),
        "no native IO was acquired by this memory-only preparation"
    );
    assert!(original.evidence().physical_confirmed());
    assert!(matches!(
        memory.reserve_driver_metadata(zero_tun::packet_memory::PacketMetadataLayout::new(1, 16),),
        Err(zero_tun::packet_memory::PacketMemoryError::Stopped)
    ));
    drop(memory);
    drop(read);
    assert_eq!(roots.packet.native().occupied.frames, 0);
    actual.begin_shutdown();
    assert!(actual.next_receipt().await.is_none());
}
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
