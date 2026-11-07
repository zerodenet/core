//! Existing frontend actor and original raw job have independent actual ACKs.
use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_work_drop_keeps_original_raw_and_cleanup_with_the_same_host_receiver() {
    timeout(Duration::from_secs(5), async {
        let book = book();
        let owner = book.command_owner();
        let connection = book.reserve(ControlCharge::Connection).unwrap();
        let local = connection.connection_budget().unwrap();
        let row = owner
            .adopt_connection_command_with_input(
                local
                    .reserve(crate::configuration::ControlConnectionCharge::PendingCommand)
                    .unwrap(),
                vec![book.reserve(ControlCharge::RequestBytes(37)).unwrap()],
            )
            .unwrap();
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let (start, gate) = oneshot::channel();
        let (entered, waiting) = oneshot::channel();
        let (release, blocking) = std::sync::mpsc::sync_channel::<()>(1);
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let retained = row.clone();
        let (_, actual) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                gate.await.unwrap();
                let observation = retained.observation();
                let raw = observation.scope_with_stop(
                    stopped.clone(),
                    retained.run_blocking(
                        move || {
                            let _ = entered.send(());
                            blocking.recv().unwrap();
                        },
                        |_| async { Ok(()) },
                    ),
                );
                tokio::pin!(raw);
                tokio::select! {biased;_=stopped.cancelled()=>{},result=&mut raw=>{result?;}}
                Ok(())
            })
            .unwrap();
        assert!(row.register_scoped(actual.clone()));
        let _ = start.send(());
        waiting.await.unwrap();
        assert_eq!(book.usage().pending_commands, 1);
        assert_eq!(book.usage().request_bytes, 37);
        let original = row.test_raw_witness().unwrap();
        assert!(!original
            .scope_identity()
            .same_scope(&actual.scope_identity()));
        stop.cancel();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(actual.matches_joined(receipt.joined()));
        assert!(!receipt.joined().registration_released());
        assert!(!original.is_joined());
        assert_eq!(book.usage().pending_commands, 1);
        scope.acknowledge(&receipt).unwrap();
        assert!(actual.try_joined().unwrap().registration_released());
        assert_eq!(
            book.usage().pending_commands,
            1,
            "actual caller ACK alone is not raw/cleanup return"
        );
        assert_eq!(
            book.usage().request_bytes,
            37,
            "real caller ACK does not refund the still-owned raw request bytes"
        );
        release.send(()).unwrap();
        original.wait_registration_released().await;
        loop {
            if book.usage().pending_commands == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(book.usage().request_bytes, 0);
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
#[tokio::test]
async fn bare_current_metadata_and_live_counter_do_not_create_a_registered_work_receiver() {
    let book = book();
    let permission = book.reserve(ControlCharge::PendingCommand).unwrap();
    let observation = ApplyObservation::new();
    observation
        .scope(async {
            assert!(ApplyObservation::current_frontend(&book).is_none());
            assert_eq!(book.usage().pending_commands, 1);
        })
        .await;
    drop(permission);
    assert_eq!(book.usage().pending_commands, 0);
    assert!(book.command_owner().retirement_complete());
}
