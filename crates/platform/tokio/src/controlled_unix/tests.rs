use super::*;
use crate::contract::{lifecycle::ChannelControl, CloseReason};
use std::{future::Future, task::Poll, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[tokio::test]
async fn pending_unix_listener_custody_blocks_native_close_until_the_same_view_returns() {
    let directory = tempfile::tempdir().unwrap();
    let owner = ControlledUnixListenerOwner::new(
        directory.path().join("control.sock"),
        CancellationToken::new(),
    );
    let mut view = owner.bind().unwrap();
    let mut accepting = Box::pin(view.accept());
    poll_fn(|cx| {
        assert!(accepting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    owner.request_stop();
    assert_eq!(
        accepting
            .await
            .err()
            .expect("original accept must stop")
            .kind(),
        io::ErrorKind::Interrupted
    );
    assert!(owner
        .close(Instant::now() + Duration::from_millis(1))
        .await
        .is_err());
    assert!(
        owner.bind().is_err(),
        "a returned error cannot surrender the retained view"
    );
    assert!(owner
        .close(Instant::now() + Duration::from_millis(1))
        .await
        .is_err());
    drop(view);
    let receipt = owner.close(end()).await.unwrap();
    assert!(owner.matches_closed(&receipt));
    assert!(receipt.had_native_acquisition());
    assert!(Arc::ptr_eq(&receipt, &owner.close(end()).await.unwrap()));
    owner.remove_bound_path(&receipt).unwrap();
}

#[tokio::test]
async fn accepted_unix_original_is_retained_before_registration_and_closes_independently() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("control.sock");
    let owner = ControlledUnixListenerOwner::new(path.clone(), CancellationToken::new());
    let mut listener = owner.bind().unwrap();
    let mut peer = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (mut pending, mut control) = listener.accept().await.unwrap();
    let mut stream = pending.activate(end()).await.unwrap();
    drop(pending);
    peer.write_all(b"actual").await.unwrap();
    let mut bytes = [0; 6];
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"actual");
    let mut receive = Box::pin(stream.read_exact(&mut bytes));
    poll_fn(|cx| {
        assert!(receive.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(receive);
    control.cancel(CloseReason::Shutdown);
    drop(stream);
    let report = control.wait_closed(end()).await.unwrap();
    assert!(report.is_complete());
    assert!(!report.closed_io.is_empty());
    drop(listener);
    let closed = owner.close(end()).await.unwrap();
    assert!(owner.matches_closed(&closed));
    owner.remove_bound_path(&closed).unwrap();
}

#[tokio::test]
async fn failed_original_bind_closes_its_own_native_and_never_unlinks_a_foreign_socket() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("control.sock");
    let fixture = std::os::unix::net::UnixListener::bind(&path).unwrap();
    let before = path_identity(&path).unwrap();
    let owner = ControlledUnixListenerOwner::new(path.clone(), CancellationToken::new());
    assert!(owner.bind().is_err());
    let receipt = owner.close(end()).await.unwrap();
    assert!(owner.matches_closed(&receipt));
    assert!(receipt.had_native_acquisition());
    owner.remove_bound_path(&receipt).unwrap();
    assert_eq!(path_identity(&path).unwrap(), before);
    let other = ControlledUnixListenerOwner::new(
        directory.path().join("other.sock"),
        CancellationToken::new(),
    );
    assert!(!other.matches_closed(&receipt));
    assert!(other.remove_bound_path(&receipt).is_err());
    drop(fixture);
}
