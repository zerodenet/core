//! A deadline-free logical close keeps the real ordered barrier until ACK.
use super::*;
use tokio::io::AsyncWriteExt;
use zero_platform_tokio::controlled_carrier::loan::StreamLoan;

#[tokio::test]
async fn deadline_free_logical_close_waits_for_real_barrier_and_preserves_same_pool_peer() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = Arc::new(ClientPool::default());
        let opener = Arc::new(Loopback::default());
        let (address, server) = echo().await;
        let (mut scope, ctx) = controlled_context(1, address, opener.clone());
        let actual = ctx.instance();
        let mut owner = PoolFactory
            .build(
                Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),
                ctx,
            )
            .unwrap();
        owner.lifecycle.start(deadline()).await.unwrap();
        let key = key_at("logical-ready", address);
        let StreamLoan {
            io: mut first,
            owner: mut control,
        } = pool
            .open_managed_tcp(actual, key.clone(), &Default::default(), None)
            .await
            .unwrap()
            .into_carrier_loan()
            .unwrap();
        first.write_all(b"a").await.unwrap();
        let mut byte = [0; 1];
        first.read_exact(&mut byte).await.unwrap();
        assert_eq!(&byte, b"a");
        let mut second = pool
            .open_managed_tcp(actual, key, &Default::default(), None)
            .await
            .unwrap();
        let original = control.original().clone();
        assert!(second.original_carrier().unwrap().same_original(&original));
        let driver = original.driver().unwrap().clone();
        assert!(
            !control.try_close().unwrap().is_complete(),
            "the unique logical view is still live"
        );
        assert!(control.closed().is_none());
        drop(first);
        loop {
            let report = control.try_close().unwrap();
            if report.is_complete() {
                assert!(report.closed_io.is_empty());
                break;
            }
            assert!(!driver.is_joined());
            assert!(original.closed().is_none());
            tokio::task::yield_now().await;
        }
        assert!(control.closed().unwrap().matches(&control));
        assert!(control.try_close().unwrap().released_resources.is_empty());
        assert!(!driver.is_joined());
        assert!(original.closed().is_none());
        second.write_all(b"b").await.unwrap();
        second.read_exact(&mut byte).await.unwrap();
        assert_eq!(&byte, b"b");
        assert_eq!(opener.opens.load(Ordering::SeqCst), 1);
        drop(second);
        let end = deadline();
        let (protocol, tasks) = tokio::join!(owner.lifecycle.shutdown(end), scope.shutdown(end));
        let protocol = protocol.unwrap();
        assert!(protocol.is_complete(), "{protocol:?}");
        assert_eq!(protocol.closed_io.len(), 1);
        assert!(tasks.is_complete() && tasks.exited_tasks.len() == 1);
        assert!(driver.is_joined());
        assert!(original.closed().unwrap().matches(&original));
        server.await.unwrap();
    })
    .await
    .unwrap();
}
