use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[tokio::test]
async fn actual_old_pending_native_flush_cannot_acknowledge_new_data_epoch() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let (mut client, mut server, cleanup) = super::test_fixture::pair().await;
        let state = Arc::new(Flushed::default());
        let mut native = NativeFlush::new(state.clone());
        client.write_all(b"old").await.unwrap();
        let mut old = [0; 3];
        server.read_exact(&mut old).await.unwrap();
        assert_eq!(&old, b"old");
        client.gate.hold();
        std::future::poll_fn(|cx| {
            assert!(native.poll(&mut client, cx).is_pending());
            Poll::Ready(())
        })
        .await;
        client.gate.blocked().await;
        let epoch = state.request().unwrap();
        client.write_all(b"new DATA").await.unwrap();
        let mut new = [0; 8];
        server.read_exact(&mut new).await.unwrap();
        assert_eq!(&new, b"new DATA");
        client.gate.release();
        std::future::poll_fn(|cx| {
            assert!(native.poll(&mut client, cx).is_pending(),
                "the actual old flush completed, but its codec continuation still owes the newer epoch");
            assert!(
                !state.ready(epoch).unwrap(),
                "the old native flush only acknowledges its starting epoch"
            );
            Poll::Ready(())
        })
        .await;
        client.gate.hold();
        std::future::poll_fn(|cx| {
            assert!(native.poll(&mut client, cx).is_pending());
            assert!(!state.ready(epoch).unwrap());
            Poll::Ready(())
        })
        .await;
        client.gate.release();
        std::future::poll_fn(|cx| native.poll(&mut client, cx))
            .await
            .unwrap();
        assert!(state.ready(epoch).unwrap());
        drop(client);
        drop(server);
        cleanup.await;
    })
    .await
    .expect("same eight-second original flush and native cleanup bound");
}

#[tokio::test]
async fn actual_pending_native_flush_is_not_acknowledged_by_successful_body_completion() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let (mut client, mut server, cleanup) = super::test_fixture::pair().await;
        let state = Arc::new(Flushed::default());
        let mut native = NativeFlush::new(state.clone());
        client.write_all(b"real original DATA").await.unwrap();
        let mut bytes = [0; 18];
        server.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"real original DATA");
        let epoch = state.request().unwrap();
        client.gate.hold();
        std::future::poll_fn(|cx| {
            assert!(native.poll(&mut client, cx).is_pending());
            Poll::Ready(())
        })
        .await;
        client.gate.blocked().await;
        // This is the actual observer path called when the original connection
        // body returns Ok. It must not publish a native flush that is Pending.
        let ended = std::future::ready(Ok(())).await;
        state.finish(ended).unwrap();
        assert!(state.completed.load(Ordering::Acquire) < epoch);
        assert_eq!(
            state.ready(epoch).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        std::future::poll_fn(|cx| {
            assert!(native.poll(&mut client, cx).is_pending());
            Poll::Ready(())
        })
        .await;
        client.gate.release();
        std::future::poll_fn(|cx| native.poll(&mut client, cx))
            .await
            .unwrap();
        assert!(
            state.ready(epoch).unwrap(),
            "only this actual native Ready advances the epoch"
        );
        drop(client);
        drop(server);
        cleanup.await;
    })
    .await
    .expect("same original eight-second native-flush bound");
}
