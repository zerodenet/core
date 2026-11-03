use super::*;
use std::future::Future;
use zero_platform_tokio::PendingUnixConnect;
#[tokio::test]
async fn original_unix_target_close_retires_the_actual_pending_read_and_held_view() {
    timeout(Duration::from_secs(5), async {
        let path = std::env::temp_dir().join(format!(
            "zero-owned-unix-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let (mut pending, mut original) =
            PendingUnixConnect::prepare(path.to_str().unwrap(), CancellationToken::new()).unwrap();
        let (connected, accepted) = tokio::join!(pending.connect(deadline()), listener.accept());
        let mut view = connected.unwrap();
        let mut peer = accepted.unwrap().0;
        view.write_all(b"one").await.unwrap();
        let mut payload = [0; 3];
        peer.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"one");
        let mut byte = [0];
        let mut read = Box::pin(view.read(&mut byte));
        std::future::poll_fn(|cx| {
            assert!(
                read.as_mut().poll(cx).is_pending(),
                "actual original read is suspended before cancellation"
            );
            std::task::Poll::Ready(())
        })
        .await;
        let (closed, read) = tokio::join!(original.wait_closed(deadline()), read);
        assert!(read.is_err());
        let receipt = closed.unwrap();
        assert!(receipt.is_complete());
        assert_eq!(receipt.closed_io.len(), 1);
        assert!(view.write_all(b"retired").await.is_err());
        assert!(original
            .wait_closed(deadline())
            .await
            .unwrap()
            .closed_io
            .is_empty());
        drop(listener);
        std::fs::remove_file(path).unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn failed_unix_target_connect_retains_its_original_close_controller() {
    let path = std::env::temp_dir().join(format!(
        "zero-absent-target-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let (mut pending, mut original) =
        PendingUnixConnect::prepare(path.to_str().unwrap(), CancellationToken::new()).unwrap();
    assert!(pending.connect(deadline()).await.is_err());
    assert!(
        pending.connect(deadline()).await.is_err(),
        "failed plan cannot respawn"
    );
    let receipt = original.wait_closed(deadline()).await.unwrap();
    assert!(receipt.is_complete());
    assert_eq!(receipt.closed_io.len(), 1);
}
