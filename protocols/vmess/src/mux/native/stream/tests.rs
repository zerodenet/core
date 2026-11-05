use super::*;
use tokio::io::AsyncWriteExt;
#[tokio::test]
async fn native_mux_tcp_upload_queue_backpressure_preserves_accepted_frames_before_end() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (socket, mut peer) = tokio::io::duplex(1);
        let (connection, mut driver) =
            VmessMuxConn::prepare(socket, 64, None, MuxResponseBacklogPolicy::default());
        let connection = Arc::new(connection);
        // Suspend the actual original physical read/write future before enqueue.
        std::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let sid = connection.try_reserve_stream_id().unwrap();
        let mut stream = connection
            .open_native_stream(sid, Address::Ipv4([203, 0, 113, 1]), 443, Network::Tcp)
            .unwrap();
        for _ in 0..8 {
            stream.write_all(b"x").await.unwrap();
        }
        std::future::poll_fn(|cx| {
            assert!(
                Pin::new(&mut stream)
                    .poll_write(cx, b"not accepted")
                    .is_pending(),
                "actual native TCP writer must wait for physical FIFO capacity"
            );
            Poll::Ready(())
        })
        .await;
        drop(stream);
        let physical = tokio::spawn(driver);
        let mut accepted = Vec::new();
        for index in 0..=8 {
            let frame = read_frame_from_tokio(&mut peer).await.unwrap();
            if index == 8 {
                assert_eq!(frame.status, MUX_STATUS_END);
            } else {
                accepted.extend(frame.payload);
            }
        }
        assert_eq!(
            accepted, b"xxxxxxxx",
            "reserved END follows every accepted frame and no pending frame was submitted"
        );
        drop(peer);
        physical.await.unwrap();
        assert!(connection.closed.load(Ordering::Acquire));
    })
    .await
    .unwrap();
}
