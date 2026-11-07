use super::*;
use std::{future::poll_fn, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn cancelled_pending_data_is_not_accepted_and_end_waits_for_the_same_fifo() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let (incoming, receiver) = mpsc::unbounded_channel();
        drop(incoming);
        let (sender, mut frames) = mpsc::channel(1);
        let mut stream = QueuedChannelStream::new(
            receiver,
            sender,
            |_: &mut Context<'_>, bytes: Option<&[u8]>| {
                Poll::Ready(Ok(bytes.unwrap_or_default().to_vec()))
            },
            || {},
            NonZeroUsize::new(8192).unwrap(),
            Vec::new(),
        );
        stream.write_all(b"accepted").await.unwrap();
        poll_fn(|cx| {
            assert!(Pin::new(&mut stream)
                .poll_write(cx, b"cancelled original poll")
                .is_pending());
            Poll::Ready(())
        })
        .await;
        // Upload EOF is independent from the blocked reverse DATA direction.
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(frames.recv().await.unwrap(), b"accepted");
        stream
            .write_all(b"replacement after cancellation")
            .await
            .unwrap();
        poll_fn(|cx| {
            assert!(Pin::new(&mut stream).poll_shutdown(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(
            frames.recv().await.unwrap(),
            b"replacement after cancellation"
        );
        stream.shutdown().await.unwrap();
        assert_eq!(frames.recv().await.unwrap(), Vec::<u8>::new());
        assert!(
            frames.try_recv().is_err(),
            "cancelled DATA and duplicate END were never accepted"
        );
    })
    .await
    .expect("original finite queue deadline");
}
