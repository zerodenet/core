//! Real asynchronous Auxiliary work and the existing Critical cleanup owner.
use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_result_does_not_refund_before_its_actual_auxiliary_and_cleanup_ack() {
    timeout(Duration::from_secs(5), async {
        let book = book();
        let owner = book.command_owner();
        let (ack, gate) = oneshot::channel();
        *lock(&owner.hold_ack) = Some(gate);
        let (entered, waiting) = oneshot::channel();
        let (release, work) = oneshot::channel();
        let (cleaning, cleanup) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let original = owner.clone();
        let actual = book.clone();
        let request = tokio::spawn(async move {
            original
                .run_async(
                    actual,
                    async move {
                        let _ = entered.send(());
                        work.await.unwrap();
                        17
                    },
                    move |_| async move {
                        let _ = cleaning.send(());
                        finished.await.unwrap();
                        Ok(())
                    },
                )
                .await
        });
        waiting.await.unwrap();
        let row = lock(&owner.state).entries[0].clone();
        let raw = row.raw.get().unwrap().clone();
        let wrapper = row.wrapper.get().unwrap().clone();
        assert!(raw.scope_identity().same_scope(&wrapper.scope_identity()));
        assert!(raw.native_task_id().is_some());
        assert!(
            row.raw_id.get().is_none(),
            "real async work owns no blocking task outcome"
        );
        assert!(!raw.is_joined());
        assert_eq!(book.usage().pending_commands, 1);
        release.send(()).unwrap();
        cleanup.await.unwrap();
        let joined = raw.wait().await;
        assert!(raw.matches_joined(&joined));
        assert!(!joined.registration_released());
        assert_eq!(book.usage().pending_commands, 1);
        assert_eq!(
            book.reserve(ControlCharge::PendingCommand)
                .err()
                .unwrap()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        finish.send(()).unwrap();
        assert_eq!(request.await.unwrap().unwrap(), 17);
        assert_eq!(book.usage().pending_commands, 1);
        ack.send(()).unwrap();
        wrapper.wait_registration_released().await;
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(owner.retirement_complete());
        assert_eq!(book.usage().pending_commands, 0);
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_async_waiter_stops_only_business_and_retains_original_cleanup() {
    timeout(Duration::from_secs(5), async {
        let book = book();
        let owner = book.command_owner();
        let (entered, waiting) = oneshot::channel();
        let (cleaning, cleanup) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let original = owner.clone();
        let actual = book.clone();
        let request = tokio::spawn(async move {
            original
                .run_async(
                    actual,
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
                )
                .await
        });
        waiting.await.unwrap();
        let row = lock(&owner.state).entries[0].clone();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        cleanup.await.unwrap();
        assert!(row.raw.get().unwrap().is_joined());
        assert!(!row.wrapper.get().unwrap().is_joined());
        assert_eq!(book.usage().pending_commands, 1);
        release.send(()).unwrap();
        row.wrapper
            .get()
            .unwrap()
            .wait_registration_released()
            .await;
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(owner.retirement_complete());
        assert_eq!(book.usage().pending_commands, 0);
    })
    .await
    .unwrap();
}

#[test]
fn async_duty_never_occupies_the_only_blocking_thread_before_its_actual_child() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        timeout(Duration::from_secs(5), async {
            let book = book();
            let owner = book.command_owner();
            let (entered, waiting) = oneshot::channel();
            let (release, gate) = oneshot::channel();
            let actual = owner.clone();
            let original = book.clone();
            let request = tokio::spawn(async move {
                actual
                    .run_async(
                        original,
                        async move {
                            entered.send(tokio::task::id()).unwrap();
                            gate.await.unwrap();
                            tokio::task::spawn_blocking(|| (tokio::task::id(), 41))
                                .await
                                .unwrap()
                        },
                        |_| async { Ok(()) },
                    )
                    .await
            });
            let auxiliary = waiting.await.unwrap();
            let row = lock(&owner.state).entries[0].clone();
            assert_eq!(row.raw.get().unwrap().native_task_id(), Some(auxiliary));
            assert!(row.raw_id.get().is_none());
            assert_eq!(book.usage().pending_commands, 1);
            release.send(()).unwrap();
            let (child, value) = request.await.unwrap().unwrap();
            assert_eq!(value, 41);
            assert_ne!(child, auxiliary);
            owner
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert!(owner.retirement_complete());
            assert_eq!(book.usage().pending_commands, 0);
        })
        .await
        .unwrap();
    });
}
