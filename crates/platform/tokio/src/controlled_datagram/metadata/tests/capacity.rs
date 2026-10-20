use super::*;
#[tokio::test]
async fn actual_native_error_is_not_logged_as_success_and_zero_segment_never_submits() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        rx: _rx,
        tx,
        mut control,
    } = owner.bind(address()).unwrap();
    let local = tx.local_addr().unwrap();
    let mut poller = tx.create_write_poller().unwrap();
    poll_fn(|cx| poller.poll_writable(cx)).await.unwrap();
    let bytes = vec![0; 65536];
    let error = tx.try_send(&transmit(local, &bytes)).unwrap_err();
    assert!(
        error.raw_os_error().is_some(),
        "original native send source survives"
    );
    let mut invalid = transmit(local, b"one");
    invalid.segment_size = Some(0);
    assert_eq!(
        tx.try_send(&invalid).unwrap_err().kind(),
        std_io::ErrorKind::InvalidInput
    );
    control.close().unwrap();
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
async fn probe_attempt_capacity_failure_closes_main_and_never_invents_smaller_capability() {
    let mut limits = limits();
    limits.probe_attempts_per_initialization = bound(1);
    // On kernels requiring GSO and GRO native tests, the second real probe
    // cannot be admitted. Old kernels may need only the GRO probe.
    if quinn_udp::controlled::capability_without_probe(quinn_udp::controlled::ProbeKind::Gso)
        .is_some()
    {
        return;
    }
    let mut owner = MetadataOwner::new(limits).unwrap();
    let error = match owner.bind(address()) {
        Ok(_) => panic!("probe budget must not fabricate GRO=1"),
        Err(error) => error,
    };
    assert_eq!(error.source.kind(), std_io::ErrorKind::WouldBlock);
    assert!(matches!(error.outcome, AcquisitionOutcome::Closed(_)));
    let report = owner.take_report();
    assert_eq!(report.main.confirmed_closed, 1);
    assert_eq!(report.probes.confirmed_closed, 1);
    assert!(report.is_complete());
}
#[test]
fn checked_operation_bound_refuses_overflow_before_any_native_acquisition() {
    let mut limits = limits();
    limits.write_pollers = bound(usize::MAX);
    assert!(
        matches!(MetadataOwner::new(limits), Err(error) if error.kind() == std_io::ErrorKind::InvalidInput)
    );
}
#[test]
fn consuming_registration_panic_retains_exact_unknown_and_refuses_replacement() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = owner.bind(address());
    }))
    .is_err());
    let first = owner.close_all();
    let retry = owner.close_all();
    assert_eq!(first.main.confirmed_closed, 0);
    assert_eq!(first.main.unconfirmed.len(), 1);
    assert!(Arc::ptr_eq(
        &first.main.unconfirmed[0].source,
        &retry.main.unconfirmed[0].source
    ));
    assert_eq!(owner.occupied_main(), 1);
    let denied = match owner.bind(address()) {
        Ok(_) => panic!("unknown original must block new acquisition"),
        Err(error) => error,
    };
    assert!(Arc::ptr_eq(
        &denied.source,
        &first.main.unconfirmed[0].source
    ));
}
#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
async fn actual_gso_submission_preserves_segment_stride_and_payload_without_replay() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        mut rx,
        tx,
        mut control,
    } = owner.bind(address()).unwrap();
    if tx.capabilities().transmit_segments < 2 {
        control.close().unwrap();
        return;
    }
    let mut message = transmit(tx.local_addr().unwrap(), b"onetwo");
    message.segment_size = Some(3);
    assert_eq!(
        send(&tx, &message).await,
        MetadataSendOutcome {
            bytes: 6,
            datagrams: 2,
            complete: true
        }
    );
    let mut payload = Vec::new();
    while payload.len() < 6 {
        let mut first = [0; 64];
        let mut second = [0; 64];
        let mut buffers = [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)];
        let mut metadata = [MetadataReceive::default(); 2];
        let count = poll_fn(|cx| rx.poll_recv(cx, &mut buffers, &mut metadata))
            .await
            .unwrap();
        for index in 0..count {
            assert_eq!(metadata[index].source, message.destination);
            assert_eq!(metadata[index].stride, 3);
            payload.extend_from_slice(&buffers[index][..metadata[index].len]);
        }
    }
    assert_eq!(payload, b"onetwo");
    control.close().unwrap();
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
#[tokio::test]
async fn actual_closed_main_and_probes_reuse_finite_slots_while_old_views_stay_revoked() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let mut old = Vec::new();
    for _ in 0..3 {
        let GrantedMetadataDatagram {
            mut rx,
            tx,
            mut control,
        } = owner.bind(address()).unwrap();
        let local = tx.local_addr().unwrap();
        assert_eq!(send(&tx, &transmit(local, b"epoch")).await.bytes, 5);
        let mut bytes = [0; 16];
        let mut buffers = [IoSliceMut::new(&mut bytes)];
        let mut metadata = [MetadataReceive::default()];
        assert_eq!(
            poll_fn(|cx| rx.poll_recv(cx, &mut buffers, &mut metadata))
                .await
                .unwrap(),
            1
        );
        assert_eq!(&buffers[0][..metadata[0].len], b"epoch");
        control.close().unwrap();
        assert_eq!(owner.occupied_main(), 0);
        assert_eq!(owner.occupied_probes(), 0);
        old.push(tx);
    }
    for tx in old {
        assert_eq!(
            tx.try_send(&transmit(address(), b"old"))
                .unwrap_err()
                .kind(),
            std_io::ErrorKind::NotConnected
        );
    }
    let report = owner.take_report();
    assert_eq!(report.main.confirmed_closed, 3);
    assert!(report.is_complete());
    assert_eq!(owner.take_report().main.confirmed_closed, 0);
}
