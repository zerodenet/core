use super::*;
use std::{
    error::Error,
    panic::{catch_unwind, AssertUnwindSafe},
};

#[tokio::test]
async fn actual_post_install_panic_retains_original_control_until_explicit_close() {
    let (mut owner, mut view, address) = bound();
    let mut accept =
        Box::pin(view.accept_owned_with(|| panic!("real accepted control already installed")));
    assert!(accept
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    let mut peer = TcpStream::connect(address).await.unwrap();
    timeout(
        Duration::from_secs(3),
        poll_fn(
            |cx| match catch_unwind(AssertUnwindSafe(|| accept.as_mut().poll(cx))) {
                Err(_) => Poll::Ready(()),
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(_)) => panic!("actual post-install hook must panic"),
            },
        ),
    )
    .await
    .unwrap();
    drop(accept);
    let evidence = owner.accepted_evidence().unwrap();
    assert_eq!(evidence.retained, 1);
    assert!(evidence.unknown.is_empty());
    assert_eq!(evidence.users, AcceptedTcpUsers::NoTransfer);
    let mut byte = [0];
    assert_eq!(
        peer.try_read(&mut byte).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let report = owner.wait_closed(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 2);
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    assert!(owner.accepted_evidence().unwrap().provider_confirmed());
}
#[tokio::test]
async fn hidden_tokio_registration_error_retains_same_original_source_unknown_slot_and_partial_listener_receipt(
) {
    let (mut owner, mut view, address) = bound();
    let dead = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = dead.handle().clone();
    dead.shutdown_background();
    let mut accept = Box::pin(view.accept_owned());
    assert!(accept
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    let _peer = TcpStream::connect(address).await.unwrap();
    let error = timeout(
        Duration::from_secs(3),
        poll_fn(|cx| {
            let _entered = handle.enter();
            accept.as_mut().poll(cx)
        }),
    )
    .await
    .unwrap()
    .err()
    .unwrap();
    drop(accept);
    assert_eq!(error.delivery, DeliveryState::Unknown);
    let first = owner.accepted_evidence().unwrap().unknown[0].1.clone();
    let mut cause = error.source();
    let mut exact = false;
    while let Some(actual) = cause {
        if let Some(actual) = actual.downcast_ref::<ChannelError>() {
            exact |= std::ptr::eq(actual, first.as_ref());
        }
        cause = actual.source();
    }
    assert!(
        exact,
        "actual registration cause is retained, not just its string or errno"
    );
    for retry in 0..2 {
        assert!(owner.wait_closed(deadline()).await.is_err());
        let evidence = owner.accepted_evidence().unwrap();
        assert_eq!(evidence.retained, 1);
        assert_eq!(evidence.confirmed_native_closes, 0);
        assert!(Arc::ptr_eq(&first, &evidence.unknown[0].1));
        assert!(!evidence.provider_confirmed());
        assert_eq!(
            owner.take_native_report().closed_io.len(),
            usize::from(retry == 0),
            "listener close receipt survives child Unknown exactly once"
        );
    }
}
#[tokio::test]
async fn hidden_tokio_registration_unwind_is_not_unused_pending_accept() {
    let (mut owner, mut view, address) = bound();
    let no_io = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut accept = Box::pin(view.accept_owned());
    assert!(accept
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    let _peer = TcpStream::connect(address).await.unwrap();
    timeout(
        Duration::from_secs(3),
        poll_fn(|cx| {
            let _entered = no_io.enter();
            match catch_unwind(AssertUnwindSafe(|| accept.as_mut().poll(cx))) {
                Err(_) => Poll::Ready(()),
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(_)) => {
                    panic!("actual accepted registration without IO driver must unwind")
                }
            }
        }),
    )
    .await
    .unwrap();
    drop(accept);
    let first = owner.accepted_evidence().unwrap().unknown[0].1.clone();
    assert!(owner.wait_closed(deadline()).await.is_err());
    let evidence = owner.accepted_evidence().unwrap();
    assert_eq!(evidence.retained, 1);
    assert_eq!(evidence.confirmed_native_closes, 0);
    assert!(Arc::ptr_eq(&first, &evidence.unknown[0].1));
    assert!(!evidence.provider_confirmed());
    // Drop the unused non-IO runtime outside this running async context.
    no_io.shutdown_background();
}
