//! Real blocking tasks and actual hosted receipts; no fabricated TaskJoined.
use super::*;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{timeout, Duration};
fn book() -> Arc<ControlBudget> {
    let mut limits = crate::configuration::ControlBudgetLimits::from_runtime(&Default::default());
    limits.messages.pending_commands = 1;
    ControlBudget::new(limits)
}
#[test]
fn cold_host_and_embedded_owner_do_not_require_a_tokio_runtime() {
    let host = crate::RuntimeHost::<()>::new(Default::default());
    assert!(host.embedded_commands().retirement_complete());
    assert!(Arc::ptr_eq(
        host.embedded_commands(),
        &host.control_budget().command_owner()
    ));
    assert_eq!(host.control_budget().usage().pending_commands, 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_waiter_keeps_original_blocking_registration_and_same_root_credit() {
    timeout(Duration::from_secs(5), async {
        let budget = book();
        let owner = budget.command_owner();
        let (entered, waiting) = oneshot::channel();
        let (release, gate) = std::sync::mpsc::sync_channel::<()>(1);
        let running = owner.clone();
        let actual = budget.clone();
        let request = tokio::spawn(async move {
            running
                .run(
                    actual,
                    move || {
                        let _ = entered.send(());
                        gate.recv().unwrap();
                        17usize
                    },
                    |_| async { Ok(()) },
                )
                .await
        });
        waiting.await.unwrap();
        let entry = lock(&owner.state).entries[0].clone();
        let raw = entry.raw.get().unwrap().clone();
        let wrapper = entry.wrapper.get().unwrap().clone();
        assert!(!raw.is_joined() && !wrapper.is_joined());
        assert!(entry.raw_id.get().is_some());
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(entry.stop.is_cancelled());
        assert_eq!(budget.usage().pending_commands, 1);
        assert!(!owner.retirement_complete());
        assert!(budget.reserve(ControlCharge::PendingCommand).is_err());
        release.send(()).unwrap();
        raw.wait_registration_released().await;
        wrapper.wait_registration_released().await;
        loop {
            if budget.usage().pending_commands == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(raw.scope_identity().same_scope(&wrapper.scope_identity()));
        assert_eq!(lock(&owner.state).limit, 1);
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(owner.retirement_complete());
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_panic_reaches_actual_host_receiver_and_a_closed_owner_can_recover() {
    timeout(Duration::from_secs(5), async {
        let budget = book();
        let owner = budget.command_owner();
        let entered = Arc::new(AtomicBool::new(false));
        let marker = entered.clone();
        let failure = owner
            .run(
                budget.clone(),
                move || -> () {
                    marker.store(true, Ordering::Release);
                    panic!("actual embedded original blocking panic")
                },
                |_| async { Ok(()) },
            )
            .await;
        assert!(failure.is_err() && entered.load(Ordering::Acquire));
        let entry = lock(&owner.state).entries[0].clone();
        let raw = entry.raw.get().unwrap();
        let wrapper = entry.wrapper.get().unwrap();
        raw.wait_registration_released().await;
        wrapper.wait_registration_released().await;
        let raw_joined = raw.try_joined().unwrap();
        let original = raw_joined.result().unwrap_err();
        assert!(Arc::ptr_eq(entry.raw_failure.get().unwrap(), &original));
        let blocking = entry.observation.blocking_outcome().unwrap();
        let panic = blocking.failure().unwrap();
        assert!(panic.is_panic());
        assert!(
            Arc::ptr_eq(panic, &super::run::raw_panic(original.as_ref()).unwrap()),
            "the actual raw receipt and public observation keep the exact JoinError Arc"
        );
        assert_eq!(panic.id(), blocking.task_id());
        assert_eq!(Some(blocking.task_id()), entry.raw_id.get().copied());
        let canonical = owner.observe_failure().await;
        assert!(canonical.source().is_some());
        assert!(
            owner
                .run(budget.clone(), || (), |_| async { Ok(()) })
                .await
                .is_err(),
            "first actual fault seals new registrations"
        );
        let cleanup = owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(owner.retirement_complete());
        assert!(cleanup.source().is_some());
        owner.reopen_after_retirement().unwrap();
        assert_eq!(
            owner
                .run(budget.clone(), || 23, |_| async { Ok(()) })
                .await
                .unwrap(),
            23
        );
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(budget.usage().pending_commands, 0);
        drop((canonical, cleanup));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_join_without_host_ack_does_not_refund_the_original_command() {
    timeout(Duration::from_secs(5), async {
        let budget = book();
        let owner = budget.command_owner();
        let (release_ack, gate) = oneshot::channel();
        *lock(&owner.hold_ack) = Some(gate);
        let (entered, waiting) = oneshot::channel();
        let (release, raw_gate) = std::sync::mpsc::sync_channel(1);
        let running = owner.clone();
        let actual = budget.clone();
        let request = tokio::spawn(async move {
            running
                .run(
                    actual,
                    move || {
                        let _ = entered.send(());
                        raw_gate.recv().unwrap();
                    },
                    |_| async { Ok(()) },
                )
                .await
        });
        waiting.await.unwrap();
        let entry = lock(&owner.state).entries[0].clone();
        let raw = entry.raw.get().unwrap().clone();
        release.send(()).unwrap();
        let joined = raw.wait().await;
        assert!(raw.matches_joined(&joined));
        assert!(!joined.registration_released());
        assert_eq!(budget.usage().pending_commands, 1);
        assert!(budget.reserve(ControlCharge::PendingCommand).is_err());
        release_ack.send(()).unwrap();
        request.await.unwrap().unwrap();
        entry
            .wrapper
            .get()
            .unwrap()
            .wait_registration_released()
            .await;
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(owner.retirement_complete());
        assert_eq!(budget.usage().pending_commands, 0);
    })
    .await
    .unwrap();
}

#[path = "tests/frontend.rs"]
mod frontend;

#[path = "tests/limits.rs"]
mod limits;

#[path = "tests/returned_scope.rs"]
mod returned_scope;

#[path = "tests/async_frontend.rs"]
mod async_frontend;
#[path = "tests/async_owned.rs"]
mod async_owned;

#[path = "tests/work_handoff.rs"]
mod work_handoff;
