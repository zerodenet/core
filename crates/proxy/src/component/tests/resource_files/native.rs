//! Actual native/file progress remains in the host across cancelled cleanup.
use super::*;
use std::{
    future::{poll_fn, Future},
    task::{Context, Poll, Waker},
};
use zero_core::{request::Destination, Address};

fn tcp() -> ResourceDeclaration {
    ResourceDeclaration::TcpListener {
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
    }
}
fn setup(path: PathBuf) -> Arc<HostResources> {
    HostResources::new(
        ComponentId(211),
        &[tcp(), ResourceDeclaration::File { path, max_bytes: 4 }],
        CancellationToken::new(),
    )
    .unwrap()
}
fn destination() -> Destination {
    Destination::new(Address::Ipv4([127, 0, 0, 1]), 0)
}

#[tokio::test]
async fn native_receipt_survives_cancelled_cleanup_while_actual_file_job_is_still_pending() {
    let directory = Directory::new();
    let path = directory.0.join("both");
    std::fs::write(&path, b"both").unwrap();
    let grants = setup(path.clone());
    let gate = Gate::new();
    let _release = gate.release_guard();
    grants.state.lock().unwrap().files.before_read = Some(gate.hook());
    let _access = grants.activate(deadline()).unwrap();
    let view = grants.listen_tcp(destination()).await.unwrap();
    let actual = view.local_addr().unwrap();
    let mut read = Box::pin(grants.read_file(path, 4));
    assert!(read
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    gate.entered().await; // Actual blocking OS-read job is held before its read.
    let mut cleanup = Box::pin(grants.shutdown_io(deadline()));
    assert!(cleanup
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    assert_eq!(grants.state.lock().unwrap().io.receipts.closed_io.len(), 1);
    assert_eq!(grants.state.lock().unwrap().files.records.len(), 1);
    // Native closure already happened even though the other branch is pending.
    std::net::TcpListener::bind(actual).unwrap();
    drop(cleanup);
    gate.release();
    let report = tokio::time::timeout_at(deadline(), grants.shutdown_io(deadline()))
        .await
        .unwrap()
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert_eq!(report.released_resources.len(), 1);
    assert!(grants
        .shutdown_io(deadline())
        .await
        .unwrap()
        .closed_io
        .is_empty());
    assert!(view.local_addr().is_err());
}

#[tokio::test]
async fn actual_file_join_receipt_survives_cancelled_cleanup_while_native_owner_is_busy() {
    let directory = Directory::new();
    let path = directory.0.join("join");
    std::fs::write(&path, b"join").unwrap();
    let grants = setup(path.clone());
    let _access = grants.activate(deadline()).unwrap();
    let view = grants.listen_tcp(destination()).await.unwrap();
    let actual = view.local_addr().unwrap();
    let gate = Gate::new();
    let _release = gate.release_guard();
    grants.state.lock().unwrap().files.before_read = Some(gate.hook());
    let mut read = Box::pin(grants.read_file(path, 4));
    assert!(read
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    gate.entered().await;
    let owner = grants
        .state
        .lock()
        .unwrap()
        .tcp
        .values()
        .next()
        .unwrap()
        .clone();
    let native = owner.lock().await;
    let mut cleanup = Box::pin(grants.shutdown_io(deadline()));
    assert!(cleanup
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    gate.release();
    tokio::time::timeout_at(
        deadline(),
        poll_fn(|cx| {
            assert!(cleanup.as_mut().poll(cx).is_pending());
            if grants.state.lock().unwrap().files.records.is_empty() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        grants
            .state
            .lock()
            .unwrap()
            .files
            .receipts
            .released_resources
            .len(),
        1
    );
    assert!(std::net::TcpListener::bind(actual).is_err());
    drop(cleanup);
    drop(native);
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert_eq!(report.released_resources.len(), 1);
    std::net::TcpListener::bind(actual).unwrap();
    drop(view);
}

#[test]
fn actual_tcp_and_udp_registration_unknowns_do_not_prevent_file_join_or_lose_original_sources() {
    let directory = Directory::new();
    let path = directory.0.join("file");
    std::fs::write(&path, b"file").unwrap();
    let grants = HostResources::new(
        ComponentId(212),
        &[
            tcp(),
            ResourceDeclaration::DatagramListener {
                destination: destination(),
                max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
                max_pending_sends: std::num::NonZeroUsize::new(1).unwrap(),
            },
            ResourceDeclaration::File {
                path: path.clone(),
                max_bytes: 4,
            },
        ],
        CancellationToken::new(),
    )
    .unwrap();
    let dead = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = dead.handle().clone();
    drop(dead);
    let _access = grants.activate(deadline()).unwrap();
    {
        let _entered = handle.enter();
        // Actual reactor registration errors consume the two originals. Even
        // if Tokio dropped them, no native close confirmation can be invented.
        let tcp_error = futures_util::FutureExt::now_or_never(grants.listen_tcp(destination()))
            .unwrap()
            .err()
            .unwrap();
        let udp_error =
            futures_util::FutureExt::now_or_never(grants.listen_datagram(destination()))
                .unwrap()
                .err()
                .unwrap();
        assert_eq!(
            tcp_error.delivery,
            zero_platform_tokio::contract::DeliveryState::Unknown
        );
        assert_eq!(
            udp_error.delivery,
            zero_platform_tokio::contract::DeliveryState::Unknown
        );
    }
    let healthy = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    healthy.block_on(async {
        assert_eq!(grants.read_file(path, 4).await.unwrap(), b"file");
        let failure = grants.shutdown_io(deadline()).await.unwrap_err();
        assert_eq!(failure.reason, Reason::Io);
        let original = {
            let state = grants.state.lock().unwrap();
            assert_eq!(state.files.receipts.released_resources.len(), 1);
            assert!(state.io.receipts.closed_io.is_empty());
            assert_eq!(state.io.failures.len(), 2);
            state.io.failures.clone()
        };
        for _ in 0..2 {
            assert!(grants.shutdown_io(deadline()).await.is_err());
            let state = grants.state.lock().unwrap();
            assert_eq!(state.files.receipts.released_resources.len(), 1);
            assert!(state.io.receipts.closed_io.is_empty());
            for (key, source) in &original {
                assert!(Arc::ptr_eq(source, state.io.failures.get(key).unwrap()));
            }
        }
    });
}

#[tokio::test]
async fn original_blocking_panic_stays_faulted_while_native_receipt_is_collected_and_retained() {
    let directory = Directory::new();
    let path = directory.0.join("panic");
    let grants = setup(path.clone());
    grants.state.lock().unwrap().files.before_read =
        Some(Arc::new(|| panic!("actual native+file blocking failure")));
    let _access = grants.activate(deadline()).unwrap();
    let view = grants.listen_tcp(destination()).await.unwrap();
    let actual = view.local_addr().unwrap();
    let read_error = grants.read_file(path, 4).await.unwrap_err();
    assert_eq!(read_error.reason, Reason::Internal);
    let original = grants.state.lock().unwrap().files.failure.clone().unwrap();
    for _ in 0..2 {
        let failure = grants.shutdown_io(deadline()).await.unwrap_err();
        assert_eq!(failure.reason, Reason::Internal);
        let mut source: &(dyn Error + 'static) = &failure;
        loop {
            if source.is::<JoinError>() {
                break;
            }
            source = source
                .source()
                .expect("actual original blocking JoinError retained");
        }
        let state = grants.state.lock().unwrap();
        assert!(state.files.records.is_empty());
        assert_eq!(state.files.receipts.released_resources.len(), 1);
        assert_eq!(state.io.receipts.closed_io.len(), 1);
        assert!(Arc::ptr_eq(
            &original,
            state.files.failure.as_ref().unwrap()
        ));
    }
    // Sticky task fault is not a reason to skip native cleanup. This actual
    // successful close receipt stays retained despite the current Err API.
    std::net::TcpListener::bind(actual).unwrap();
    assert!(view.local_addr().is_err());
}
