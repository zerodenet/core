//! A later real sibling exit cannot turn the first ACK wait into an IO fault.
use super::*;
use std::{future::Future, task::Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[tokio::test]
async fn later_original_worker_failure_joins_the_same_pending_host_ack_wait() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let n = NonZeroUsize::new(2).unwrap();
        let mut scope = TaskScope::suspended_hosted(Handle::current(), Some(n));
        scope.enable_registration().unwrap();
        let tasks = OwnedDnsQueryTasks::new_critical(&scope.registrar(), n);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut releases = Vec::new();
        for _ in 0..2 {
            let release = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut reader, _) = listener.accept().await.unwrap();
            let (entered, ready) = oneshot::channel();
            tasks
                .spawn(
                    async move {
                        entered.send(()).unwrap();
                        reader.read_exact(&mut [0]).await.unwrap();
                        panic!("actual separately released DNS sibling");
                    },
                    CancellationToken::new(),
                )
                .unwrap();
            ready.await.unwrap();
            releases.push(release);
        }
        let originals = tasks.observe();
        assert_eq!(originals.len(), 2);
        releases[0].write_all(&[1]).await.unwrap();
        let first = scope.next_receipt().await.unwrap();
        let caller = &originals
            .iter()
            .find(|actual| actual.task.matches_joined(first.joined()))
            .unwrap()
            .task;
        tasks.reap();
        let first_source = first.result().unwrap_err();
        let mut cleanup =
            Box::pin(tasks.close_descendants(caller, Instant::now() + Duration::from_secs(1)));
        std::future::poll_fn(|cx| {
            assert!(cleanup.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        releases[1].write_all(&[1]).await.unwrap();
        let later = scope.next_receipt().await.unwrap();
        let later_source = later.result().unwrap_err();
        tasks.reap();
        assert_eq!(tasks.retained_faults().unwrap().faults().len(), 2);
        scope.acknowledge(&first).unwrap();
        std::future::poll_fn(|cx| {
            assert!(
                cleanup.as_mut().poll(cx).is_pending(),
                "the later exact source waits for its own ACK"
            );
            Poll::Ready(())
        })
        .await;
        assert!(first.joined().registration_released());
        assert!(!later.joined().registration_released());
        scope.acknowledge(&later).unwrap();
        cleanup.await.unwrap();
        let retained = tasks.retained_faults().unwrap();
        assert_eq!(retained.faults().len(), 2);
        assert!(retained
            .faults()
            .iter()
            .any(|fault| Arc::ptr_eq(&fault.error, &first_source)));
        assert!(retained
            .faults()
            .iter()
            .any(|fault| Arc::ptr_eq(&fault.error, &later_source)));
        assert!(scope.is_drained());
        assert!(tasks.ensure_open().is_err());
    })
    .await
    .unwrap();
}
