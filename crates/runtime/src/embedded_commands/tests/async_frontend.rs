//! The same real frontend registration, without an added execution task.
use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_async_work_survives_actual_caller_ack_until_its_original_cleanup_returns() {
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
        let (entered, waiting) = oneshot::channel();
        let (cleaning, cleanup) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let actual = row.clone();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                begun.await.unwrap();
                let observation = actual.observation();
                let command = observation.scope_with_stop(
                    stopping.clone(),
                    actual.run_async(
                        async move {
                            let stop = ApplyObservation::caller_stop().unwrap();
                            let _ = entered.send(());
                            stop.cancelled().await;
                        },
                        move |_| async move {
                            let _ = cleaning.send(());
                            gate.await.unwrap();
                            Ok(())
                        },
                    ),
                );
                tokio::pin!(command);
                tokio::select! {biased;_=stopping.cancelled()=>{},result=&mut command=>{result?;}}
                Ok(())
            })
            .unwrap();
        assert!(row.register_scoped(witness.clone()));
        start.send(()).unwrap();
        waiting.await.unwrap();
        assert!(
            row.test_raw_witness().is_none(),
            "CURRENT did not create a raw execution task"
        );
        assert!(row.test_async_started());
        stop.cancel();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(witness.matches_joined(receipt.joined()));
        scope.acknowledge(&receipt).unwrap();
        cleanup.await.unwrap();
        assert!(witness.try_joined().unwrap().registration_released());
        assert_eq!(book.usage().pending_commands, 1);
        assert!(
            !row.test_async_returned(),
            "caller ACK is not the actual async cleanup return"
        );
        release.send(()).unwrap();
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
async fn original_async_panic_payload_reaches_the_real_owner_and_allows_physical_recovery() {
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
        let (done, result) = oneshot::channel();
        let actual = row.clone();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                begun.await.unwrap();
                let observation = actual.observation();
                let failure = observation
                    .scope(actual.run_async(
                        async {
                            std::panic::panic_any(String::from("same original async panic"));
                        },
                        |_| async { Ok(()) },
                    ))
                    .await
                    .unwrap_err();
                let _ = done.send(failure);
                Ok(())
            })
            .unwrap();
        assert!(row.register_scoped(witness.clone()));
        start.send(()).unwrap();
        let error = result.await.unwrap();
        let source = row.test_async_failure().unwrap();
        assert!(source
            .source()
            .unwrap()
            .downcast_ref::<AsyncCommandPanic>()
            .unwrap()
            .is::<String>());
        assert!(row.test_blocking_id().is_none());
        assert!(row.test_raw_witness().is_none());
        let receipt = scope.next_receipt().await.unwrap();
        assert!(witness.matches_joined(receipt.joined()));
        scope.acknowledge(&receipt).unwrap();
        loop {
            if book.usage().pending_commands == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let retired = owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(owner.retirement_complete());
        let faults = retired
            .source()
            .unwrap()
            .downcast_ref::<EmbeddedFaultSet>()
            .unwrap();
        assert!(faults.0.iter().any(|fault| fault
            .raw
            .as_ref()
            .is_some_and(|old| Arc::ptr_eq(old, &source))));
        owner.reopen_after_retirement().unwrap();
        assert_eq!(
            owner
                .run_async(book.clone(), async { 23 }, |_| async { Ok(()) })
                .await
                .unwrap(),
            23
        );
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            error
                .source()
                .unwrap()
                .downcast_ref::<Arc<ChannelError>>()
                .unwrap(),
            &source
        ));
        drop(retired);
        drop(source);
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
