use super::*;

#[tokio::test]
async fn actual_reactor_registration_failure_keeps_original_for_matching_explicit_close() {
    let file = std::fs::File::open("/dev/null").unwrap();
    let owner = AdoptedTunOwner::new();
    let failure = owner
        .create_with(
            Some("registration-fixture"),
            false,
            |_| Ok(file.into()),
            |_, _| Ok("registration-fixture".into()),
        )
        .err()
        .expect("epoll rejects /dev/null as an async input");
    assert_eq!(failure.source.raw_os_error(), Some(libc::EPERM));
    assert!(Arc::ptr_eq(
        &failure.source,
        &owner.acquisition_failure().unwrap()
    ));
    let TunAcquisitionOutcome::Closed(receipt) = failure.outcome else {
        panic!("reactor error lost the native original before explicit close")
    };
    let original = receipt.identity();
    assert!(original.closed().unwrap().matches(original));
    assert!(Arc::ptr_eq(&receipt, &owner.close().unwrap()));
    let rejected = owner.create_with(
        None,
        false,
        |_| panic!("no native replay"),
        |_, _| unreachable!(),
    );
    assert!(matches!(
        rejected.err().unwrap().outcome,
        TunAcquisitionOutcome::Closed(_)
    ));
}
