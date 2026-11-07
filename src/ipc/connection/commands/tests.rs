//! Two real original command exits survive a cancelled connection's full drain.
use super::*;
#[tokio::test]
async fn command_io_failure_and_panic_both_keep_their_actual_typed_results() {
    let original = Arc::new(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "original command writer failed",
    ));
    let mut tasks = ConcurrentCommandTasks::new(CancellationToken::new());
    let (first, entered) = tokio::sync::oneshot::channel();
    let fault = original.clone();
    tasks.tasks.spawn(async move {
        first.send(()).unwrap();
        Err::<(), _>(io::Error::new(fault.kind(), fault))
    });
    let (second, panicking) = tokio::sync::oneshot::channel();
    fn panic_command() -> io::Result<()> {
        std::panic::panic_any("actual concurrent command panic")
    }
    tasks.tasks.spawn(async move {
        second.send(()).unwrap();
        panic_command()
    });
    entered.await.unwrap();
    panicking.await.unwrap();
    while tasks.failures.len() < 2 {
        tasks.reap_finished();
        tokio::task::yield_now().await;
    }
    assert_eq!(tasks.failures.len(), 2);
    let returned = tasks
        .failures
        .iter()
        .find_map(|error| error.get_ref()?.downcast_ref::<Arc<io::Error>>())
        .unwrap();
    assert!(Arc::ptr_eq(returned, &original));
    assert!(tasks
        .failures
        .iter()
        .find_map(|error| error.get_ref()?.downcast_ref::<tokio::task::JoinError>())
        .unwrap()
        .is_panic());
    assert!(!tasks.try_spawn(async { Ok(()) }));
    assert!(tasks.cancel_all().await.is_err());
    assert!(tasks.tasks.is_empty());
}

#[tokio::test]
async fn actual_command_failure_wakes_cleanup_without_waiting_for_another_request() {
    for panic in [false, true] {
        let stop = CancellationToken::new();
        let mut tasks = ConcurrentCommandTasks::new(stop.clone());
        let (entered, started) = tokio::sync::oneshot::channel();
        assert!(tasks.try_spawn(async move {
            entered.send(()).unwrap();
            std::future::pending::<io::Result<()>>().await
        }));
        started.await.unwrap();
        fn panic_command() -> io::Result<()> {
            std::panic::panic_any("actual command panic wakes original connection")
        }
        assert!(tasks.try_spawn(async move {
            if panic {
                panic_command()
            } else {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "actual command output failure",
                ))
            }
        }));
        stop.cancelled().await;
        let returned = tasks.cancel_all().await.unwrap_err();
        if panic {
            assert!(returned
                .get_ref()
                .unwrap()
                .downcast_ref::<tokio::task::JoinError>()
                .unwrap()
                .is_panic());
        } else {
            assert_eq!(returned.kind(), io::ErrorKind::PermissionDenied);
        }
        assert!(tasks.tasks.is_empty());
    }
}

#[tokio::test]
async fn body_done_keeps_root_command_and_request_credit_until_original_join_is_received() {
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.pending_commands = 1;
    let budget = ControlBudget::new(limits);
    let mut tasks =
        ConcurrentCommandTasks::test_with_budget(CancellationToken::new(), budget.clone());
    let request = budget.reserve(ControlCharge::RequestBytes(7)).unwrap();
    let (entered, body) = tokio::sync::oneshot::channel();
    assert!(tasks.try_spawn_owned(
        async move {
            entered.send(()).unwrap();
            Ok(())
        },
        vec![request]
    ));
    body.await.unwrap();
    assert_eq!(tasks.tasks.len(), 1);
    assert_eq!(budget.usage().pending_commands, 1);
    assert_eq!(budget.usage().request_bytes, 7);
    assert!(budget.reserve(ControlCharge::PendingCommand).is_err());
    tasks.cancel_all().await.unwrap();
    assert_eq!(budget.usage().pending_commands, 0);
    assert_eq!(budget.usage().request_bytes, 0);
}
