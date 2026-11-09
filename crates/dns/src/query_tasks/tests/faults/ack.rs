//! The actual Host ACK wait is not another protocol/native cleanup failure.
use super::*;
use std::{future::Future, task::Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn received_worker_panic_waits_for_its_actual_host_ack_without_republishing_the_source() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut release = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut reader, _) = listener.accept().await.unwrap();
        let n = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::suspended_hosted(Handle::current(), Some(n));
        scope.enable_registration().unwrap();
        let tasks = OwnedDnsQueryTasks::new_critical(&scope.registrar(), n);
        let (entered, ready) = oneshot::channel();
        tasks
            .spawn(
                async move {
                    entered.send(()).unwrap();
                    reader.read_exact(&mut [0]).await.unwrap();
                    panic!("actual DNS worker panic after original socket IO");
                },
                CancellationToken::new(),
            )
            .unwrap();
        ready.await.unwrap();
        let original = tasks.observe().pop().unwrap().task;
        assert!(!original.is_joined());
        release.write_all(&[1]).await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        let joined = receipt.joined();
        assert!(original.matches_joined(joined));
        let panic = joined.result().unwrap_err();
        tasks.reap();
        let received = tasks.retained_faults().unwrap();
        assert_eq!(received.faults().len(), 1);
        assert!(Arc::ptr_eq(&received.faults()[0].error, &panic));
        assert!(joined.failure_accepted());
        assert!(!joined.registration_released());
        assert!(!scope.is_drained());

        // This is our own expired observation, not a second copy of the panic.
        let pending = tasks
            .close_descendants(&original, Instant::now())
            .await
            .unwrap_err();
        assert!(pending
            .get_ref()
            .unwrap()
            .is::<crate::DnsRetirementPending>());
        assert!(!joined.registration_released());
        assert_eq!(tasks.retained_faults().unwrap().faults().len(), 1);

        let mut cleanup =
            Box::pin(tasks.close_descendants(&original, Instant::now() + Duration::from_secs(1)));
        for _ in 0..3 {
            std::future::poll_fn(|cx| {
                assert!(cleanup.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let retained = tasks.retained_faults().unwrap();
            assert_eq!(retained.faults().len(), 1);
            assert!(Arc::ptr_eq(&retained.faults()[0].error, &panic));
            assert!(!joined.registration_released());
            assert!(
                tasks.ensure_open().is_err(),
                "waiting does not reopen the failed original pool"
            );
        }
        scope.acknowledge(&receipt).unwrap();
        cleanup.await.unwrap();
        assert!(joined.registration_released());
        assert!(scope.is_joined());
        assert!(scope.unjoined_tasks().is_empty());
        assert!(
            !scope.is_drained(),
            "registration remains open until its owner closes it"
        );
        scope.begin_shutdown();
        assert!(scope.is_drained());
        assert!(Arc::ptr_eq(&joined.result().unwrap_err(), &panic));
        assert!(Arc::ptr_eq(&received.faults()[0].error, &panic));
        assert_eq!(tasks.retained_faults().unwrap().faults().len(), 1);
        assert!(tasks.ensure_open().is_err());
    })
    .await
    .unwrap();
}
