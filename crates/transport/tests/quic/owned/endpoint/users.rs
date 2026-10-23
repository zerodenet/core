use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn actual_sealed_quinn_stream_users_hold_capacity_until_raw_drop_then_real_join_and_native_close(
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(3));
        let (mut owner, mut receiver, handle, retained) = owner(&scope, 4);
        let local = handle.local_addr().unwrap();
        let (client, server) = tokio::join!(handle.connect(local, "localhost"), handle.accept());
        let client = client.unwrap();
        let server = server.unwrap();
        assert_eq!(owner.active_users(), 2);
        let mut send = client.open_uni().await.unwrap();
        let ((), receive) = tokio::join!(
            async {
                send.write_all(b"owned user bytes").await.unwrap();
            },
            server.accept_uni(),
        );
        let mut receive = receive.unwrap();
        let mut payload = [0; 16];
        receive.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"owned user bytes");
        assert_eq!(owner.active_users(), 4);
        assert_eq!(client.try_clone().err().unwrap().reason, Reason::Capacity);
        assert_eq!(
            owner.driver_tasks().len(),
            3,
            "user pressure did not register a fallback driver"
        );
        drop(client);
        assert_eq!(owner.active_users(), 3);
        let reused = server.try_clone().unwrap();
        assert_eq!(
            owner.active_users(),
            4,
            "actual handle release permits healthy capacity reuse"
        );
        drop(reused);
        assert_eq!(owner.active_users(), 3);
        drop(server);
        assert_eq!(
            owner.active_users(),
            2,
            "stream references still own actual users"
        );
        let mut next = [0; 1];
        let mut read = Box::pin(receive.read(&mut next));
        poll_fn(|cx| {
            assert!(read.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        owner.begin_shutdown();
        assert_eq!(read.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(
            receive.read_u8().await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            send.write_all(b"retired").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        // Cancellation returning does not destroy either original stream.
        assert_eq!(owner.active_users(), 2);
        assert_eq!(
            owner.shutdown(Instant::now()).await.err().unwrap().reason,
            Reason::Timeout
        );
        assert!(
            retained.local_addr().is_ok(),
            "pending users prevented native close"
        );
        drop(send);
        drop(receive);
        assert_eq!(owner.active_users(), 0);
        let (report, classes) = tokio::join!(
            owner.shutdown(Instant::now() + Duration::from_secs(3)),
            collect(&mut scope, &mut receiver, 3),
        );
        let report = report.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.confirmed_main, 1);
        assert_eq!(
            classes
                .iter()
                .filter(|class| **class == TaskClass::Critical)
                .count(),
            1
        );
        assert_eq!(
            classes
                .iter()
                .filter(|class| **class == TaskClass::Auxiliary)
                .count(),
            2
        );
        assert_eq!(
            retained.local_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        let retry = owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(retry.is_complete());
        assert!(
            Arc::ptr_eq(&report.native, &retry.native),
            "retry retains the same actual native receipt snapshot"
        );
        assert_eq!(retry.confirmed_main, 1);
        assert!(handle.accept().await.is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_pending_accept_cancellation_releases_its_endpoint_clone_without_driver_or_io_ack() {
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let (mut owner, mut receiver, handle, retained) = owner(&scope, 1);
    let mut accepting = Box::pin(handle.accept());
    poll_fn(|cx| {
        assert!(accepting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(owner.active_users(), 1);
    assert_eq!(handle.local_addr().err().unwrap().reason, Reason::Capacity);
    owner.begin_shutdown();
    assert_eq!(accepting.await.err().unwrap().reason, Reason::Cancelled);
    assert_eq!(owner.active_users(), 0);
    assert!(retained.local_addr().is_ok());
    assert_eq!(owner.driver_status().unjoined.len(), 1);
    let (report, classes) = tokio::join!(
        owner.shutdown(Instant::now() + Duration::from_secs(2)),
        collect(&mut scope, &mut receiver, 1),
    );
    assert_eq!(classes, [TaskClass::Critical]);
    assert!(report.unwrap().is_complete());
    assert_eq!(
        retained.local_addr().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}
