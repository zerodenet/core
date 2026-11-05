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
        let mut stream = StreamReservation::new(
            connection.clone(),
            sid,
            443,
            &Address::Ipv4([203, 0, 113, 1]),
        )
        .open()
        .await
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

#[tokio::test]
async fn native_mux_stream_reopen_waits_for_reserved_end_and_cancel_returns_sid() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let (socket, mut peer) = tokio::io::duplex(1);
        let (connection, driver) = MuxPoolConn::prepare(
            socket,
            &[0; 16],
            1,
            None,
            MuxResponseBacklogPolicy::default(),
            false,
        );
        let connection = Arc::new(connection);
        let target = Address::Ipv4([203, 0, 113, 1]);
        let first_sid = connection.try_reserve_stream_id().unwrap();
        let mut first = StreamReservation::new(connection.clone(), first_sid, 443, &target)
            .open()
            .await
            .unwrap();
        first.write_all(b"first").await.unwrap();
        drop(first);
        // The logical SID returned, but its real FIFO END still holds the
        // sole control permit while the original driver has not been polled.
        let cancelled_sid = connection.try_reserve_stream_id().unwrap();
        let mut cancelled = Box::pin(
            StreamReservation::new(connection.clone(), cancelled_sid, 443, &target).open(),
        );
        std::future::poll_fn(|cx| {
            assert!(cancelled.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(cancelled);
        let fresh_sid = connection
            .try_reserve_stream_id()
            .expect("cancel returns the admitted SID");
        let mut fresh =
            Box::pin(StreamReservation::new(connection.clone(), fresh_sid, 443, &target).open());
        std::future::poll_fn(|cx| {
            assert!(fresh.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let physical = tokio::spawn(driver);
        let new = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(new.session_id, first_sid);
        assert_eq!(new.status, crate::mux::STATUS_NEW);
        let data = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(data.session_id, first_sid);
        assert_eq!(data.payload, b"first");
        let end = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(end.session_id, first_sid);
        assert_eq!(end.status, crate::mux::STATUS_END);
        let mut fresh = fresh.await.unwrap();
        fresh.write_all(b"after").await.unwrap();
        let new = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(new.session_id, fresh_sid, "cancelled admission sent no NEW");
        assert_eq!(new.status, crate::mux::STATUS_NEW);
        let data = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(data.session_id, fresh_sid);
        assert_eq!(data.payload, b"after");
        drop(fresh);
        let end = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(end.session_id, fresh_sid);
        assert_eq!(end.status, crate::mux::STATUS_END);
        drop(peer);
        physical.await.unwrap();
        assert!(connection.closed());
    })
    .await
    .unwrap();
}
