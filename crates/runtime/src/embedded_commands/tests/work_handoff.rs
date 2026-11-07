//! Complete waits the same operation and every other captured producer;
//! its own observing frontend is handed off, never treated as a join receipt.
use super::*;
async fn actual_handoff(blocking: bool) {
    timeout(Duration::from_secs(5), async {
        let book = book();
        let owner = book.command_owner();
        let row = owner
            .adopt_command(book.reserve(ControlCharge::PendingCommand).unwrap())
            .unwrap();
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let (start, begun) = oneshot::channel();
        let (saved, other_producer) = oneshot::channel();
        let (cleaning, cleanup) = oneshot::channel();
        let (done, returned) = oneshot::channel();
        let actual = row.clone();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                begun.await.unwrap();
                let observation = actual.observation();
                let result = observation
                    .scope(async {
                        let work = move || {
                            let guard = ApplyObservation::capture().unwrap();
                            assert!(saved.send(guard).is_ok());
                            17
                        };
                        let complete = move |observation: ApplyObservation| async move {
                            let _ = cleaning.send(());
                            assert!(
                                observation.wait_joined().await.is_none(),
                                "only same completed work proves KnownNoAcceptedWork"
                            );
                            let _ = done.send(());
                            Ok(())
                        };
                        if blocking {
                            actual.run_blocking(work, complete).await
                        } else {
                            actual.run_async(async move { work() }, complete).await
                        }
                    })
                    .await?;
                assert_eq!(result, 17);
                Ok(())
            })
            .unwrap();
        assert!(row.register_scoped(witness.clone()));
        start.send(()).unwrap();
        let guard = other_producer.await.unwrap();
        cleanup.await.unwrap();
        let mut returned = returned;
        assert!(
            matches!(
                returned.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ),
            "a different genuine producer was not exempted"
        );
        assert!(!witness.is_joined());
        assert_eq!(book.usage().pending_commands, 1);
        drop(guard);
        returned.await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(witness.matches_joined(receipt.joined()));
        assert!(!receipt.joined().registration_released());
        assert_eq!(book.usage().pending_commands, 1);
        scope.acknowledge(&receipt).unwrap();
        loop {
            if book.usage().pending_commands == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(owner.retirement_complete());
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_blocking_complete_waits_real_other_producers_without_waiting_its_own_observer() {
    actual_handoff(true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_async_complete_waits_real_other_producers_without_waiting_its_own_observer() {
    actual_handoff(false).await;
}
