use super::*;
#[tokio::test]
async fn original_read_capacity_is_rejected_before_native_acquisition_and_retains_real_driver() {
    let mut actual = scope();
    let roots = Arc::new(crate::HostOriginalBudgets::default());
    roots.packet.install_native(1, 65_535).unwrap();
    let original = owner_with_budget(&actual, i32::MAX, "not-a-tun", roots.clone());
    let (send, pending, witness) = driver(&actual);
    pending.await.unwrap();
    let failure = failed(original.open(witness.clone()));
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
