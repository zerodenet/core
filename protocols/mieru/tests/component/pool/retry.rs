use super::*;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn no_caller_deadline_still_settles_a_real_retired_original_before_reusing_one_slot() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = Arc::new(ClientPool::default());
        let (address, server) = echo_connections(2).await;
        let (mut scope, ctx) = controlled_context(1, address, Arc::new(Loopback::default()));
        let actual = ctx.instance();
        let mut owner = PoolFactory
            .build(
                Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),
                ctx,
            )
            .unwrap();
        owner.lifecycle.start(deadline()).await.unwrap();
        let state = pool.managed(actual).unwrap();
        let mut first = pool
            .open_managed_tcp(actual, key_at("one", address), &Default::default(), None)
            .await
            .unwrap();
        first.write_all(b"first").await.unwrap();
        first.read_exact(&mut [0; 5]).await.unwrap();
        pool.clear();
        drop(first);
        let exit = scope.next_exit().await.unwrap();
        assert!(exit.result.is_ok());
        assert!(
            state.physical_unconfirmed(),
            "actual join alone is not native close"
        );
        assert_eq!(
            state.reserve().err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut second = pool
            .open_managed_tcp(actual, key_at("two", address), &Default::default(), None)
            .await
            .unwrap();
        second.write_all(b"again").await.unwrap();
        let mut bytes = [0; 5];
        second.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"again");
        drop(second);
        pool.clear();
        let (protocol, tasks) = tokio::join!(
            owner.lifecycle.shutdown(deadline()),
            scope.shutdown(deadline())
        );
        let report = protocol.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(
            report.closed_io.len(),
            1,
            "already accepted retirements do not accumulate as shutdown history"
        );
        assert!(tasks.is_complete(), "{tasks:?}");
        server.await.unwrap();
    })
    .await
    .unwrap();
}
