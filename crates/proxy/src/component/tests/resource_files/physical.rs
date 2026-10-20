use super::*;
use zero_platform_tokio::file::FileNativeEvidence;

#[cfg(target_os = "linux")]
fn original_is_open(path: &Path) -> bool {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .any(|entry| std::fs::read_link(entry.unwrap().path()).is_ok_and(|actual| actual == path))
}

#[tokio::test]
async fn post_open_panic_joins_the_actual_worker_then_explicitly_closes_its_original_file() {
    let directory = Directory::new();
    let path = directory.0.join("installed-before-panic");
    std::fs::write(&path, b"four").unwrap();
    let grants = grant(path.clone(), 4);
    let observed = path.clone();
    grants.state.lock().unwrap().files.after_open = Some(Arc::new(move || {
        #[cfg(target_os = "linux")]
        assert!(original_is_open(&observed));
        let _ = &observed;
        panic!("actual panic with original File in persistent holder");
    }));
    let _access = grants.activate(deadline()).unwrap();
    let read_failure = grants.read_file(path.clone(), 4).await.unwrap_err();
    assert_eq!(read_failure.reason, Reason::Internal);
    let original = grants.state.lock().unwrap().files.failure.clone().unwrap();
    let proof = grants.io_evidence();
    assert!(proof.physical_confirmed());
    assert!(proof.files[0].job_joined);
    assert!(matches!(
        proof.files[0].read.native,
        FileNativeEvidence::ConfirmedClosed
    ));
    assert!(Arc::ptr_eq(&original, &proof.sticky_faults[0]));
    #[cfg(target_os = "linux")]
    assert!(!original_is_open(&path));
    for _ in 0..2 {
        let failure = grants.shutdown_io(deadline()).await.unwrap_err();
        let mut source: &(dyn Error + 'static) = &failure;
        while !source.is::<JoinError>() {
            source = source.source().expect("original JoinError");
        }
        let state = grants.state.lock().unwrap();
        assert_eq!(state.files.receipts.closed_io.len(), 1);
        assert_eq!(state.files.receipts.released_resources.len(), 1);
        assert!(Arc::ptr_eq(
            &original,
            state.files.failure.as_ref().unwrap()
        ));
    }
}

#[tokio::test]
async fn cancelled_post_open_waiter_retains_the_actual_file_and_worker_until_join_and_close() {
    let directory = Directory::new();
    let path = directory.0.join("held-open");
    std::fs::write(&path, b"four").unwrap();
    let grants = grant(path.clone(), 4);
    let gate = Gate::new();
    let _release = gate.release_guard();
    grants.state.lock().unwrap().files.after_open = Some(gate.hook());
    let _access = grants.activate(deadline()).unwrap();
    let reading = grants.clone();
    let actual = path.clone();
    let request = tokio::spawn(async move { reading.read_file(actual, 4).await });
    gate.entered().await;
    assert!(!request.is_finished());
    #[cfg(target_os = "linux")]
    assert!(original_is_open(&path));
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let report = grants
        .shutdown_io(Instant::now() + Duration::from_millis(20))
        .await
        .unwrap();
    assert!(!report.is_complete());
    assert!(report.closed_io.is_empty());
    let proof = grants.io_evidence(); // must not block on worker-held mutex
    assert!(!proof.physical_confirmed());
    assert!(!proof.files[0].job_joined);
    assert!(matches!(
        proof.files[0].read.native,
        FileNativeEvidence::Busy
    ));
    #[cfg(target_os = "linux")]
    assert!(original_is_open(&path));
    gate.release();
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert_eq!(report.released_resources.len(), 1);
    assert!(grants.io_evidence().physical_confirmed());
    #[cfg(target_os = "linux")]
    assert!(!original_is_open(&path));
    assert!(grants
        .shutdown_io(deadline())
        .await
        .unwrap()
        .closed_io
        .is_empty());
}

#[tokio::test]
async fn no_acquisition_and_read_error_have_distinct_physical_proofs_and_original_sources() {
    let directory = Directory::new();
    let absent = directory.0.join("absent");
    let grants = grant(absent.clone(), 4);
    let _access = grants.activate(deadline()).unwrap();
    assert!(grants.read_file(absent, 4).await.is_err());
    let proof = grants.io_evidence();
    assert!(proof.physical_confirmed());
    assert!(proof.files[0].job_joined);
    assert!(matches!(
        proof.files[0].read.native,
        FileNativeEvidence::KnownNoAcquisition
    ));
    assert!(proof.files[0]
        .read
        .read_source
        .as_ref()
        .unwrap()
        .raw_os_error()
        .is_some());
    assert!(grants
        .shutdown_io(deadline())
        .await
        .unwrap()
        .closed_io
        .is_empty());
    let actual = directory.0.join("too-large");
    std::fs::write(&actual, b"large").unwrap();
    let grants = grant(actual.clone(), 4);
    let _access = grants.activate(deadline()).unwrap();
    assert_eq!(
        grants.read_file(actual, 4).await.unwrap_err().reason,
        Reason::Capacity
    );
    let proof = grants.io_evidence();
    assert!(proof.physical_confirmed());
    assert!(matches!(
        proof.files[0].read.native,
        FileNativeEvidence::ConfirmedClosed
    ));
    let source = proof.files[0].read.read_source.as_ref().unwrap();
    assert_eq!(source.kind(), io::ErrorKind::InvalidData);
    assert!(Arc::ptr_eq(
        source,
        grants.io_evidence().files[0]
            .read
            .read_source
            .as_ref()
            .unwrap()
    ));
    assert_eq!(
        grants
            .shutdown_io(deadline())
            .await
            .unwrap()
            .closed_io
            .len(),
        1
    );
}
