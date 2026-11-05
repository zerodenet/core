use super::*;
use tokio::io::AsyncWriteExt;
#[tokio::test]
async fn native_mux_tcp_upload_queue_backpressure_preserves_accepted_frames_before_end() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (socket, mut peer) = tokio::io::duplex(1);
        let (connection, mut driver) = MuxPoolConn::prepare(
            socket,
            &[0; 16],
            64,
            None,
            MuxResponseBacklogPolicy::default(),
            false,
        );
        let connection = Arc::new(connection);
        std::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let sid = connection.try_reserve_stream_id().unwrap();
        let mut stream = connection
            .open_bounded_tcp_stream(sid, 443, &Address::Ipv4([203, 0, 113, 1]))
            .unwrap();
        std::future::poll_fn(|cx| stream.poll_native_initial(cx))
            .await
            .unwrap();
        for _ in 0..7 {
            stream.write_all(b"x").await.unwrap();
        }
        std::future::poll_fn(|cx| {
            assert!(
                Pin::new(&mut stream)
                    .poll_write(cx, b"not accepted")
                    .is_pending(),
                "NEW and DATA charge one physical FIFO"
            );
            Poll::Ready(())
        })
        .await;
        drop(stream);
        let physical = tokio::spawn(driver);
        let first = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(first.status, crate::mux::STATUS_NEW);
        let mut accepted = Vec::new();
        for index in 0..8 {
            let frame = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
            if index == 7 {
                assert_eq!(frame.status, crate::mux::STATUS_END);
            } else {
                accepted.extend(frame.payload);
            }
        }
        assert_eq!(accepted, b"xxxxxxx");
        drop(peer);
        physical.await.unwrap();
        assert!(connection.closed());
    })
    .await
    .unwrap();
}
