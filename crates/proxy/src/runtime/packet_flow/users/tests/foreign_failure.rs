//! The original caller/supervisor accepts each cause; the callee owns no ACK.
use super::*;
#[tokio::test]
async fn each_failed_original_caller_stays_charged_until_its_own_acceptance_and_registration_ack() {
    let users = FlowUsers::new(NonZeroUsize::new(2).unwrap());
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(3));
    scope.enable_registration().unwrap();
    let (send_first, first_gate) = tokio::sync::oneshot::channel();
    let (send_second, second_gate) = tokio::sync::oneshot::channel();
    let (_, first) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            first_gate.await.unwrap();
            Err(
                ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                    .with_source(std::io::Error::from_raw_os_error(111)),
            )
        })
        .unwrap();
    let (_, second) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            second_gate.await.unwrap();
            Err(ChannelError::new(Stage::Send, Reason::Io, Impact::Session)
                .with_source(std::io::Error::from_raw_os_error(104)))
        })
        .unwrap();
    drop(
        users
            .acquire(Some(FlowCaller {
                witness: first.clone(),
            }))
            .unwrap(),
    );
    drop(
        users
            .acquire(Some(FlowCaller {
                witness: second.clone(),
            }))
            .unwrap(),
    );
    send_first.send(()).unwrap();
    send_second.send(()).unwrap();
    let one = scope.next_receipt().await.unwrap();
    let two = scope.next_receipt().await.unwrap();
    let (first_exit, second_exit) = if first.matches_joined(one.joined()) {
        (one, two)
    } else {
        (two, one)
    };
    assert!(second.matches_joined(second_exit.joined()));
    // These two exact Arc originals are now strongly held by the REAL receiver.
    let first_source = first_exit.result().unwrap_err();
    let second_source = second_exit.result().unwrap_err();
    assert!(users.shutdown(deadline()).await.is_err());
    assert_eq!(users.slots.active(), 2);
    assert!(Arc::ptr_eq(
        users.state.lock().unwrap().first.as_ref().unwrap(),
        &first_source
    ));
    first
        .acknowledge_hosted_failure(first_exit.joined())
        .unwrap();
    assert!(users.shutdown(deadline()).await.is_err());
    assert_eq!(users.slots.active(), 2);
    scope.acknowledge(&first_exit).unwrap();
    first.wait_registration_released().await;
    assert!(users.shutdown(deadline()).await.is_err());
    assert_eq!(users.slots.active(), 1);
    assert!(Arc::ptr_eq(
        users.state.lock().unwrap().first.as_ref().unwrap(),
        &second_source
    ));
    assert!(!second_exit.local_failure_accepted());
    second
        .acknowledge_hosted_failure(second_exit.joined())
        .unwrap();
    assert!(users.shutdown(deadline()).await.is_err());
    assert_eq!(users.slots.active(), 1);
    scope.acknowledge(&second_exit).unwrap();
    second.wait_registration_released().await;
    let report = users.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
    assert!(report.exited_tasks.is_empty());
    assert_eq!(users.slots.active(), 0);
    assert!(users.state.lock().unwrap().first.is_none());
    assert!(Arc::ptr_eq(
        &first_source,
        &first_exit.result().unwrap_err()
    ));
    assert!(Arc::ptr_eq(
        &second_source,
        &second_exit.result().unwrap_err()
    ));
}
#[tokio::test]
async fn an_unaccepted_foreign_caller_error_refuses_new_views_without_revoking_a_healthy_existing_view(
) {
    let users = FlowUsers::new(NonZeroUsize::new(3).unwrap());
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(3));
    scope.enable_registration().unwrap();
    let (release, live) = tokio::sync::oneshot::channel();
    let (_, healthy) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            live.await.unwrap();
            Ok(())
        })
        .unwrap();
    let view = users
        .acquire(Some(FlowCaller {
            witness: healthy.clone(),
        }))
        .unwrap();
    let (_, failed) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async {
            Err(
                ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                    .with_source(std::io::Error::from_raw_os_error(111)),
            )
        })
        .unwrap();
    drop(
        users
            .acquire(Some(FlowCaller {
                witness: failed.clone(),
            }))
            .unwrap(),
    );
    let exit = scope.next_receipt().await.unwrap();
    let source = exit.result().unwrap_err();
    let rejected = failed_user(users.acquire(None));
    assert_eq!(
        (rejected.stage, rejected.reason, rejected.impact),
        (source.stage, source.reason, source.impact)
    );
    assert!(!users.cancellation().is_cancelled());
    assert!(users.validates(&view));
    assert!(!healthy.is_joined());
    assert_eq!(users.slots.active(), 2);
    failed.acknowledge_hosted_failure(exit.joined()).unwrap();
    scope.acknowledge(&exit).unwrap();
    failed.wait_registration_released().await;
    users.reap();
    assert_eq!(users.slots.active(), 1);
    assert!(users.validates(&view));
    assert!(users.state.lock().unwrap().first.is_none());
    drop(view);
    release.send(()).unwrap();
    let exit = scope.next_receipt().await.unwrap();
    assert!(users.shutdown(deadline()).await.unwrap().is_complete());
    scope.acknowledge(&exit).unwrap();
}
#[tokio::test]
async fn a_missing_original_host_ack_never_becomes_callee_retirement_even_after_its_scope_is_dropped(
) {
    let users = FlowUsers::new(limit());
    let mut scope = hosted();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async {
            Err(
                ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                    .with_source(std::io::Error::from_raw_os_error(111)),
            )
        })
        .unwrap();
    drop(
        users
            .acquire(Some(FlowCaller {
                witness: witness.clone(),
            }))
            .unwrap(),
    );
    let exit = scope.next_receipt().await.unwrap();
    let original = exit.result().unwrap_err();
    drop(scope);
    assert!(!exit.local_failure_accepted());
    for _ in 0..2 {
        assert!(users.shutdown(deadline()).await.is_err());
        assert_eq!(users.slots.active(), 1);
        assert!(!users.quiescent());
        assert!(Arc::ptr_eq(
            users.state.lock().unwrap().first.as_ref().unwrap(),
            &original
        ));
    }
}
