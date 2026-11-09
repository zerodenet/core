//! Cleanup cancellation retains both actual children and the collected fault.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[tokio::test]
async fn cancelled_subscription_cleanup_keeps_actual_children_and_original_fault() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut peer = tokio::net::TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (mut wire, _) = listener.accept().await.unwrap();
        let source = Arc::new(io::Error::other("original subscription writer failure"));
        let original = source.clone();
        let writer = tokio::spawn(async move {
            wire.read_exact(&mut [0]).await?;
            Err(io::Error::new(original.kind(), original))
        });
        let (release, waiting) = std::sync::mpsc::sync_channel(1);
        let (entered, started) = tokio::sync::oneshot::channel();
        let bridge = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            waiting.recv().unwrap();
        });
        started.await.unwrap();
        let writer_id = writer.id();
        let bridge_id = bridge.id();
        let mut jobs = EventJobs {
            bridge: Some(bridge), writer: Some(writer),
            writer_result: None, bridge_result: None,
        };
        {
            let joining = jobs.join();
            tokio::pin!(joining);
            tokio::select! { biased;
                result = &mut joining => panic!("real children still pending: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }
        }
        assert_eq!(jobs.writer.as_ref().unwrap().id(), writer_id);
        assert_eq!(jobs.bridge.as_ref().unwrap().id(), bridge_id);
        assert!(jobs.writer_result.is_none() && jobs.bridge_result.is_none());
        peer.write_all(b"x").await.unwrap();
        while !jobs.writer.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
        {
            let joining = jobs.join();
            tokio::pin!(joining);
            tokio::select! { biased;
                result = &mut joining => panic!("original bridge still pending: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }
        }
        assert!(jobs.writer.is_none());
        assert_eq!(jobs.bridge.as_ref().unwrap().id(), bridge_id);
        let received = jobs.writer_result.as_ref().unwrap().as_ref().unwrap_err().clone();
        assert!(Arc::ptr_eq(received.get_ref().unwrap().downcast_ref::<Arc<io::Error>>().unwrap(), &source));
        release.send(()).unwrap();
        let [writer, bridge] = jobs.join().await;
        assert!(bridge.is_ok() && jobs.writer.is_none() && jobs.bridge.is_none());
        let returned = writer.unwrap_err();
        assert!(Arc::ptr_eq(returned.get_ref().unwrap().downcast_ref::<Arc<io::Error>>().unwrap(), &received));
        let [again, bridge] = jobs.join().await;
        assert!(bridge.is_ok());
        assert!(Arc::ptr_eq(again.unwrap_err().get_ref().unwrap().downcast_ref::<Arc<io::Error>>().unwrap(), &received));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    }).await.unwrap();
}
