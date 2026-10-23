use super::*;

#[tokio::test]
async fn controlled_carrier_early_retirement_cannot_reopen_old_reservation_or_release_new_slot() {
    let mut fixture = Fixture::new(1).await;
    let old = fixture.grants.reserve().unwrap();
    let record = old.adoption();
    assert!(record.closed().is_none());
    let report = record.retire(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(
        report.closed_io.is_empty(),
        "no native acquisition occurred"
    );
    let unused = record.closed().unwrap();
    assert!(unused.matches(&record));
    assert!(unused.driver_exit().is_none());

    let next = fixture.grants.reserve().unwrap();
    let next_record = next.adoption();
    assert!(!unused.matches(&next_record));
    let failure = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context(), old)
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::Revoked);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert!(next_record.closed().is_none());
    assert_eq!(
        fixture.owner.retained(),
        1,
        "old Drop cannot release new slot"
    );
    assert_eq!(
        fixture.grants.reserve().err().unwrap().reason,
        Reason::Capacity
    );
    drop(next);
    assert!(next_record.closed().unwrap().matches(&next_record));
    assert!(
        timeout(Duration::from_millis(20), fixture.listener.accept())
            .await
            .is_err()
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
}

#[tokio::test]
async fn controlled_carrier_record_before_prepare_survives_cancel_until_original_native_close() {
    let mut fixture = Fixture::new(1).await;
    let grant = fixture.grants.reserve().unwrap();
    let record = grant.adoption();
    let context = context();
    let cancel = context.cancel.clone();
    let prepared = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context, grant)
        .unwrap();
    assert!(record.closed().is_none());
    assert!(record.slot.state().control.is_some());
    cancel.cancel();
    let failure = prepared.open(deadline()).await.err().unwrap();
    assert_eq!(failure.reason, Reason::Cancelled);
    assert!(record.closed().is_none());
    assert_eq!(
        fixture.grants.reserve().err().unwrap().reason,
        Reason::Capacity
    );
    assert!(!record.retire(Instant::now()).await.unwrap().is_complete());
    assert!(record.closed().is_none());
    let report = record.retire(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io.len(),
        1,
        "same original control explicitly closed"
    );
    let receipt = record.closed().unwrap();
    assert!(receipt.matches(&record));
    assert!(receipt.driver_exit().is_none(), "no driver was registered");
    let next = fixture.grants.reserve().unwrap();
    assert!(!receipt.matches(&next.adoption()));
    drop(next);
    assert!(
        timeout(Duration::from_millis(20), fixture.listener.accept())
            .await
            .is_err()
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
}

#[tokio::test]
async fn controlled_carrier_retire_during_prepared_open_waits_for_original_owner_to_exit() {
    let mut fixture = Fixture::new(1).await;
    let grant = fixture.grants.reserve().unwrap();
    let record = grant.adoption();
    let prepared = fixture
        .transport
        .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
        .unwrap();
    let report = record.retire(Instant::now()).await.unwrap();
    assert!(!report.is_complete());
    assert!(record.closed().is_none());
    assert_eq!(
        fixture.grants.reserve().err().unwrap().reason,
        Reason::Capacity
    );
    assert!(
        prepared.open(deadline()).await.is_err(),
        "retired native owner cannot deliver IO"
    );
    assert!(record.closed().is_none());
    let report = record.retire(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(record.closed().unwrap().matches(&record));
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
}

#[tokio::test]
async fn controlled_carrier_real_acquisition_panic_retains_unknown_and_never_mints_early_receipt() {
    #[derive(Debug)]
    struct BoundaryPanic;
    impl zero_traits::IoObserver for BoundaryPanic {
        fn stream_boundary(&self) {
            panic!("observer panic after native socket acquisition")
        }
        fn received(&self, _: usize) {}
        fn sent(&self, _: usize) {}
        fn error(&self) {}
        fn dropped(&self) {}
    }
    let mut fixture = Fixture::new(1).await;
    let registration = fixture.owner.registration();
    let binding = fixture.transport.binding().clone();
    let opener = NativeCarrierOpener::new(
        instance(),
        binding.clone(),
        fixture.listener.local_addr().unwrap(),
        fixture.egress.clone(),
        Some(Arc::new(BoundaryPanic)),
    )
    .unwrap();
    let transport = UpstreamTransport::new(binding, Arc::new(opener));
    let grant = fixture.grants.reserve().unwrap();
    let record = grant.adoption();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transport.prepare_controlled_stream(fixture.destination.clone(), context(), grant)
    }));
    assert!(result.is_err());
    assert!(record.slot.state().control.is_none());
    let original = record
        .slot
        .state()
        .unknown
        .clone()
        .expect("unobserved ownership result persists");
    for _ in 0..2 {
        let failure = record.retire(deadline()).await.err().unwrap();
        assert_eq!(failure.delivery, DeliveryState::Unknown);
        assert!(
            record.closed().is_none(),
            "implicit socket Drop never confirms closure"
        );
        assert!(Arc::ptr_eq(
            record.slot.state().unknown.as_ref().unwrap(),
            &original
        ));
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
    }
    let snapshot = registration.observe().unwrap();
    assert!(Arc::ptr_eq(
        snapshot.slots()[0].acquisition_unknown().unwrap(),
        &original
    ));
    assert!(snapshot.slots()[0].native().is_none());
    assert!(fixture.owner.shutdown(deadline()).await.is_err());
    assert!(registration.closed().is_none());
    assert!(Arc::ptr_eq(
        record.slot.state().unknown.as_ref().unwrap(),
        &original
    ));
    assert!(
        timeout(Duration::from_millis(20), fixture.listener.accept())
            .await
            .is_err()
    );
}
