use super::*;
use std::{
    future::Future,
    task::{Context, Waker},
};

#[tokio::test]
#[ignore = "actual native ICMP access required; root runs explicitly on loopback"]
async fn pending_receive_revoke_and_view_drop_retain_original_until_actual_close() {
    let _serial = LIVE.lock().await;
    let mut owner = owner(1);
    let mut view = owner.bind(Ipv4Addr::LOCALHOST.into(), None).unwrap();
    let mut bytes = [0; 128];
    let mut pending = Box::pin(view.recv_from(&mut bytes));
    assert!(pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    owner.revoke();
    assert!(pending.await.is_err());
    assert_eq!(owner.occupied(), 1);
    let receipt = owner.close(&view).unwrap();
    assert!(receipt.matches(&view));
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert!(view.recv_from(&mut bytes).await.is_err());
    let mut next = super::owner(1);
    let view = next.bind(local(), None).unwrap();
    drop(view);
    assert_eq!(next.occupied(), 1, "view Drop did not close original");
    assert_eq!(next.close_all().confirmed_closed, 1);
}
#[tokio::test]
#[ignore = "actual native ICMP access required"]
async fn same_entry_receipt_rejects_other_pool_and_old_view_never_closes_replacement() {
    let _serial = LIVE.lock().await;
    let mut first = owner(1);
    let mut second = owner(1);
    let a = first.bind(local(), None).unwrap();
    let b = second.bind(local(), None).unwrap();
    assert_eq!(a.reservation(), b.reservation());
    assert!(first.close(&b).is_err());
    assert_eq!(first.occupied(), 1);
    assert_eq!(second.occupied(), 1);
    let receipt = first.close(&a).unwrap();
    assert!(receipt.matches(&a));
    assert!(!receipt.matches(&b));
    assert_eq!(first.take_report().confirmed_closed, 1);
    let replacement = first.bind(local(), None).unwrap();
    assert_ne!(replacement.reservation(), a.reservation());
    assert!(first.close(&a).unwrap().matches(&a));
    let report = first.take_report();
    assert_eq!(report.confirmed_closed, 0);
    assert_eq!(report.retained_live, 1);
    drop(a);
    assert!(
        replacement.local_addr().is_ok(),
        "old-view Drop cannot cancel a replacement"
    );
    assert_eq!(first.close_all().confirmed_closed, 1);
    assert!(
        b.local_addr().is_ok(),
        "other owner keeps its original live"
    );
    assert_eq!(second.close_all().confirmed_closed, 1);
}
#[tokio::test]
#[ignore = "actual native ICMP access required"]
async fn post_create_panic_leaves_same_native_original_for_explicit_owner_cleanup() {
    let _serial = LIVE.lock().await;
    let mut owner = owner(1);
    let entry = owner.reserve(false).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = owner.bind_entry(entry.clone(), local(), None, || {
            panic!("actual created ICMP original installed")
        });
    }));
    assert!(result.is_err());
    assert!(matches!(*entry.native(), Native::Raw(_)));
    #[cfg(unix)]
    let original = {
        use std::os::fd::AsRawFd;
        let native = entry.native();
        let Native::Raw(socket) = &*native else {
            unreachable!()
        };
        let fd = socket.as_raw_fd();
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        assert_eq!(unsafe { libc::fstat(fd, stat.as_mut_ptr()) }, 0);
        (fd, unsafe { stat.assume_init() }.st_ino)
    };
    let report = owner.close_all();
    assert!(report.is_complete());
    assert_eq!(report.confirmed_closed, 1);
    #[cfg(unix)]
    {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::fstat(original.0, stat.as_mut_ptr()) } == 0 {
            // Numeric descriptors can be reused by other parallel test cases.
            assert_ne!(unsafe { stat.assume_init() }.st_ino, original.1);
        } else {
            assert_eq!(
                std_io::Error::last_os_error().raw_os_error(),
                Some(libc::EBADF)
            );
        }
    }
}
#[tokio::test]
#[ignore = "actual native ICMP access and absent TEST-NET local address required"]
async fn actual_post_create_bind_error_preserves_source_and_closes_before_reuse() {
    let _serial = LIVE.lock().await;
    let mut owner = owner(1);
    let error = owner
        .bind(Ipv4Addr::new(192, 0, 2, 254).into(), None)
        .err()
        .unwrap();
    assert!(error.source.raw_os_error().is_some());
    assert!(matches!(error.outcome, IcmpAcquisitionOutcome::Closed(_)));
    let report = owner.take_report();
    assert_eq!(report.confirmed_closed, 1);
    assert!(Arc::ptr_eq(
        &error.source,
        report.first_acquisition_failure.as_ref().unwrap()
    ));
    let view = owner.bind(local(), None).unwrap();
    owner.close(&view).unwrap();
    let report = owner.take_report();
    assert_eq!(report.confirmed_closed, 1);
    assert!(Arc::ptr_eq(
        &error.source,
        report.first_acquisition_failure.as_ref().unwrap()
    ));
}
#[test]
#[ignore = "actual native ICMP access required before dead-reactor registration"]
fn consuming_registration_failure_is_permanent_unknown_with_original_source() {
    let _serial = LIVE.blocking_lock();
    let dead = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = dead.handle().clone();
    drop(dead);
    let mut owner = owner(2);
    let error = {
        let _entered = handle.enter();
        owner.bind(local(), None).err().unwrap()
    };
    assert!(matches!(error.outcome, IcmpAcquisitionOutcome::Unknown(_)));
    assert!(matches!(
        owner.bind(local(), None).err().unwrap().outcome,
        IcmpAcquisitionOutcome::Unknown(_)
    ));
    for _ in 0..2 {
        let report = owner.close_all();
        assert!(!report.is_complete());
        assert_eq!(report.confirmed_closed, 0);
        assert_eq!(report.unconfirmed.len(), 1);
        assert!(Arc::ptr_eq(&error.source, &report.unconfirmed[0].source));
        assert!(Arc::ptr_eq(
            &error.source,
            report.unconfirmed[0].acquisition_source.as_ref().unwrap()
        ));
    }
}
