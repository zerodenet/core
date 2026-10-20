use super::*;
use std::{future::Future, task::Waker, time::Duration};
use tokio::{io::AsyncReadExt, sync::oneshot, time::timeout};
mod callbacks;
mod errors;
mod ownership;
mod wake;

fn scope(limit: usize) -> HostedScope {
    let mut owner = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(limit));
    owner.enable_registration().unwrap();
    owner
}
async fn next(owner: &mut HostedScope) -> HostedExitReceipt {
    timeout(Duration::from_secs(2), owner.next_receipt())
        .await
        .unwrap()
        .unwrap()
}
fn counts(owner: &HostedScope) -> (usize, usize, usize) {
    let state = owner.0 .0.state.lock().unwrap();
    (
        state.records.len(),
        state.completed.len(),
        state.tasks.len(),
    )
}
fn pending(owner: &mut HostedScope) {
    assert!(owner
        .poll_exit(&mut Context::from_waker(Waker::noop()))
        .is_pending());
}

#[tokio::test]
async fn hosted_build_registration_requires_the_installed_owners_explicit_start() {
    let mut owner = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    let registrar = owner.registrar();
    assert_eq!(
        registrar.spawn(TaskClass::Critical, async {
            panic!("cold task was polled")
        }),
        Err(SpawnError::NotStarted)
    );
    assert!(owner.is_joined());
    assert!(
        !owner.is_drained(),
        "open registration is not a closed scope"
    );
    owner.enable_registration().unwrap();
    let id = registrar
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let receipt = next(&mut owner).await;
    assert_eq!(receipt.id(), id);
    assert_eq!(receipt.class(), TaskClass::Auxiliary);
    assert_eq!(counts(&owner), (0, 1, 0));
    assert!(owner.is_joined());
    assert!(!owner.is_drained());
    owner.begin_shutdown();
    assert_eq!(owner.enable_registration(), Err(SpawnError::Closing));
    assert!(!owner.is_drained(), "joined but unacknowledged");
    owner.acknowledge(&receipt).unwrap();
    assert!(owner.is_drained());
    assert!(owner.next_receipt().await.is_none());
    assert_eq!(
        registrar.spawn(TaskClass::Critical, async {
            panic!("closed task was polled")
        }),
        Err(SpawnError::Closing)
    );
}

#[tokio::test]
async fn none_declares_zero_tasks_without_an_implicit_host_budget() {
    let mut owner = TaskScope::suspended_hosted(Handle::current(), None);
    owner.enable_registration().unwrap();
    assert_eq!(
        owner
            .registrar()
            .spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    owner.begin_shutdown();
    assert!(owner.next_receipt().await.is_none());
    assert!(owner.is_drained());
}

#[tokio::test]
async fn observed_and_lost_receipts_keep_the_original_scope_budget_until_exact_ack() {
    let mut owner = scope(1);
    let registrar = owner.registrar();
    let (id, witness) = registrar
        .spawn_tracked(TaskClass::Critical, async { Ok(()) })
        .unwrap();
    let receipt = next(&mut owner).await;
    let original = receipt.joined().clone();
    assert_eq!(witness.wait().await.id(), id);
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    pending(&mut owner); // A live receipt is not concurrently reoffered.
    drop(receipt);
    assert_eq!(counts(&owner), (0, 1, 0));
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    let redelivered = next(&mut owner).await;
    assert!(original.same_receipt(redelivered.joined()));
    owner.acknowledge(&redelivered).unwrap();
    assert_eq!(counts(&owner), (0, 0, 0));
    let second = registrar
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    assert_ne!(id, second);
    assert_eq!(
        owner.acknowledge(&redelivered),
        Err(HostedReceiptError::StaleReceipt)
    );
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity),
        "old receipt cannot refund the new task"
    );
    let second = next(&mut owner).await;
    owner.acknowledge(&second).unwrap();
    assert!(original.result().is_ok(), "read-only evidence survives ack");
    owner.begin_shutdown();
    assert!(owner.is_drained());
}

#[tokio::test]
async fn a_foreign_receipt_never_consumes_either_scopes_actual_exit() {
    let mut left = scope(1);
    let mut right = scope(1);
    let left_id = left
        .registrar()
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let right_id = right
        .registrar()
        .spawn(TaskClass::Critical, async { Ok(()) })
        .unwrap();
    let a = next(&mut left).await;
    let b = next(&mut right).await;
    assert_ne!(left_id, right_id, "same real allocator as ordinary scopes");
    assert_eq!(left.acknowledge(&b), Err(HostedReceiptError::ForeignScope));
    assert_eq!(right.acknowledge(&a), Err(HostedReceiptError::ForeignScope));
    assert_eq!(counts(&left), (0, 1, 0));
    assert_eq!(counts(&right), (0, 1, 0));
    left.acknowledge(&a).unwrap();
    right.acknowledge(&b).unwrap();
    left.begin_shutdown();
    right.begin_shutdown();
    assert!(left.is_drained());
    assert!(right.is_drained());
}

#[tokio::test]
async fn retaining_one_offer_does_not_hide_a_second_actual_critical_exit() {
    let mut owner = scope(2);
    let first = owner
        .registrar()
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let held = next(&mut owner).await;
    let second = owner
        .registrar()
        .spawn(TaskClass::Critical, async { panic!("second actual task") })
        .unwrap();
    let other = next(&mut owner).await;
    assert_eq!(held.id(), first);
    assert_eq!(other.id(), second);
    assert_eq!(other.class(), TaskClass::Critical);
    assert!(other.result().is_err());
    assert_eq!(counts(&owner), (0, 2, 0));
    owner.acknowledge(&other).unwrap();
    owner.acknowledge(&held).unwrap();
    owner.begin_shutdown();
    assert!(owner.is_drained());
}

#[tokio::test]
async fn cancelling_a_real_pending_join_wait_keeps_the_native_task_and_its_receipt_for_retry() {
    let mut owner = scope(1);
    let (mut io, mut peer) = tokio::io::duplex(1);
    let (entered, entered_rx) = oneshot::channel();
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let reading = io.read_u8();
            tokio::pin!(reading);
            let mut entered = Some(entered);
            poll_fn(|cx| {
                let result = reading.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        entered.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .unwrap();
            Ok(())
        })
        .unwrap();
    entered_rx.await.unwrap(); // Actual read was polled Pending, not a timer.
    {
        let mut waiting = Box::pin(owner.next_receipt());
        assert!(waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
    }
    owner.begin_shutdown(); // Cancellation is not native task exit.
    pending(&mut owner);
    assert!(!owner.is_joined());
    assert!(!witness.is_joined());
    assert_eq!(counts(&owner), (1, 0, 1));
    use tokio::io::AsyncWriteExt;
    peer.write_u8(7).await.unwrap();
    let receipt = next(&mut owner).await;
    assert_eq!(receipt.id(), id);
    assert_eq!(witness.wait().await.id(), id);
    assert!(owner.is_joined());
    assert!(!owner.is_drained());
    owner.acknowledge(&receipt).unwrap();
    assert!(owner.is_drained());
}

#[tokio::test]
async fn acknowledged_success_cycles_leave_no_permanent_task_or_receipt_history() {
    let mut owner = scope(1);
    let mut previous = None;
    for _ in 0..256 {
        let (id, witness) = owner
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
            .unwrap();
        let receipt = next(&mut owner).await;
        if let Some(previous) = previous {
            assert!(id.0 > previous);
        }
        previous = Some(id.0);
        assert!(receipt.joined().same_receipt(&witness.wait().await));
        owner.acknowledge(&receipt).unwrap();
        drop(receipt);
        drop(witness);
        assert_eq!(counts(&owner), (0, 0, 0));
        assert!(owner.0 .0.state.lock().unwrap().closing_report.is_none());
    }
    owner.begin_shutdown();
    assert!(owner.is_drained());
}
