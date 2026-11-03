use super::*;
use std::{future::Future, task::Poll};
use tokio::{io::AsyncReadExt, net::TcpListener, time::Duration};

#[tokio::test]
async fn expired_probe_budget_keeps_the_same_original_until_its_later_native_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let (mut pending, original) =
        PendingTcpConnect::prepare_listener_probe(address, cancel.child_token()).unwrap();
    let stream = pending
        .connect(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    drop((stream, pending));
    let mut probe = Probe::new(NonZeroUsize::new(1).unwrap());
    probe.original = Some(original);
    let error = probe
        .run(address, Instant::now(), &cancel)
        .await
        .unwrap_err();
    assert_eq!(error.stage, Stage::Prepare);
    assert_eq!(error.reason, Reason::Timeout);
    let incomplete = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<IncompleteClose>()
        .expect("same receiver's actual incomplete report");
    assert!(!incomplete.0.is_complete());
    assert!(!incomplete.0.unfinished.is_empty());
    assert!(probe.original.is_some());
    assert_eq!(
        probe.remaining, 1,
        "elapsed budget cannot acquire a replacement"
    );
    assert_eq!(probe.confirmed, 0);
    let mut byte = [0];
    let mut reading = Box::pin(peer.read(&mut byte));
    std::future::poll_fn(|cx| {
        assert!(
            reading.as_mut().poll(cx).is_pending(),
            "original TCP is still physically open"
        );
        Poll::Ready(())
    })
    .await;
    drop(reading);
    // A fresh cleanup observation uses the original retained controller. Only
    // this matching native receipt removes it; timeout never did so above.
    probe
        .close(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(probe.original.is_none());
    assert!(probe.confirmed > 0);
    let report = probe.take_report();
    assert!(report.is_complete());
    assert!(!report.closed_io.is_empty());
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    let mut accepting = Box::pin(listener.accept());
    std::future::poll_fn(|cx| {
        assert!(
            accepting.as_mut().poll(cx).is_pending(),
            "no replacement was connected"
        );
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn expired_first_probe_budget_does_not_allocate_an_original() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut probe = Probe::new(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let error = probe
        .run(listener.local_addr().unwrap(), Instant::now(), &cancel)
        .await
        .unwrap_err();
    assert_eq!(error.stage, Stage::Prepare);
    assert_eq!(error.reason, Reason::Timeout);
    assert_eq!(probe.remaining, 1);
    assert!(probe.original.is_none());
    assert_eq!(probe.confirmed, 0);
}
