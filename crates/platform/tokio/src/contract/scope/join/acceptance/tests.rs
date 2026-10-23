use super::*;
use std::{error::Error, io, time::Duration};
use tokio::{sync::oneshot, time::timeout};

fn hosted(limit: usize) -> HostedScope {
    let mut scope = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(limit));
    scope.enable_registration().unwrap();
    scope
}
async fn next(scope: &mut HostedScope) -> HostedExitReceipt {
    timeout(Duration::from_secs(2), scope.next_receipt())
        .await
        .unwrap()
        .unwrap()
}
fn failure() -> ComponentError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
        .with_source(io::Error::other("actual local task failure"))
}

mod wake;

#[tokio::test]
async fn local_receiver_acceptance_shares_the_original_join_but_only_host_ack_returns_credit() {
    let mut scope = hosted(1);
    let registrar = scope.registrar();
    let (id, witness) = registrar
        .spawn_tracked(TaskClass::Auxiliary, async { Err(failure()) })
        .unwrap();
    assert!(witness.is_hosted());
    let receipt = next(&mut scope).await;
    let joined = witness.wait().await;
    let source = joined.result().unwrap_err();
    assert_eq!(joined.id(), id);
    assert!(Arc::ptr_eq(&source, &receipt.result().unwrap_err()));
    assert!(
        !receipt.local_failure_accepted(),
        "reading the real witness is not delivery"
    );
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );

    // Same shape as the protocol's bounded physical record: persist the exact
    // joined evidence and its source before granting explicit local delivery.
    let receiver = Arc::new(Mutex::new(Some(joined.clone())));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let receiver = receiver.clone();
        let witness = witness.clone();
        threads.push(std::thread::spawn(move || {
            let retained = receiver.lock().unwrap().as_ref().unwrap().clone();
            witness.acknowledge_hosted_failure(&retained).unwrap();
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(receipt.local_failure_accepted());
    assert!(Arc::ptr_eq(
        &source,
        &receiver
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .result()
            .unwrap_err()
    ));
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );

    scope.acknowledge(&receipt).unwrap();
    witness.acknowledge_hosted_failure(&joined).unwrap(); // same receiver, idempotent
    let other = registrar
        .spawn(TaskClass::Critical, async { Ok(()) })
        .unwrap();
    assert_ne!(id, other);
    assert_eq!(
        scope.acknowledge(&receipt),
        Err(HostedReceiptError::StaleReceipt)
    );
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    let other = next(&mut scope).await;
    scope.acknowledge(&other).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
}

#[tokio::test]
async fn foreign_unjoined_and_successful_join_receipts_do_not_grant_local_failure_acceptance() {
    let mut scope = hosted(2);
    let registrar = scope.registrar();
    let (release, waiting) = oneshot::channel();
    let (entered, observed) = oneshot::channel();
    let (_, pending) = registrar
        .spawn_tracked(TaskClass::Auxiliary, async move {
            entered.send(()).unwrap();
            waiting.await.unwrap();
            Err(failure())
        })
        .unwrap();
    observed.await.unwrap();
    let (_, success) = registrar
        .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let good = next(&mut scope).await;
    let success_join = success.wait().await;
    assert_eq!(
        pending.acknowledge_hosted_failure(&success_join),
        Err(HostedReceiptError::Unjoined)
    );
    assert_eq!(
        success.acknowledge_hosted_failure(&success_join),
        Err(HostedReceiptError::NotFailure)
    );
    assert!(!good.local_failure_accepted());
    release.send(()).unwrap();
    let failed = next(&mut scope).await;
    let failed_join = pending.wait().await;
    assert_eq!(
        pending.acknowledge_hosted_failure(&success_join),
        Err(HostedReceiptError::ForeignTask)
    );
    assert_eq!(
        success.acknowledge_hosted_failure(&failed_join),
        Err(HostedReceiptError::ForeignTask)
    );
    assert!(!failed.local_failure_accepted());
    scope.acknowledge(&good).unwrap();
    pending.acknowledge_hosted_failure(&failed_join).unwrap();
    assert!(failed.local_failure_accepted());
    scope.acknowledge(&failed).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
}

#[tokio::test]
async fn ordinary_scope_failure_acceptance_is_a_validated_no_op_with_original_timing_and_source() {
    let mut owner = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let registrar = owner.registrar();
    let native = Arc::new(io::Error::other("ordinary original source"));
    let retained = native.clone();
    let (_, witness) = registrar
        .spawn_tracked(TaskClass::Auxiliary, async move {
            Err(ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(native))
        })
        .unwrap();
    assert!(!witness.is_hosted());
    assert!(!witness.is_joined());
    let event = owner.next_exit().await.unwrap();
    let joined = witness.wait().await;
    let source = joined.result().unwrap_err();
    assert!(Arc::ptr_eq(
        source
            .source()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap(),
        &retained
    ));
    witness.acknowledge_hosted_failure(&joined).unwrap();
    witness.acknowledge_hosted_failure(&joined).unwrap();
    assert!(!joined.failure_accepted());
    // Ordinary delivery already returned its existing task budget, and did
    // not acquire hosted receipt/source semantics or an earlier join.
    registrar
        .spawn(TaskClass::Critical, async { Ok(()) })
        .unwrap();
    assert!(event.result.is_err());
    let report = owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 1);
}

#[tokio::test]
async fn dropping_a_real_local_record_without_explicit_acceptance_keeps_the_scope_charge() {
    let mut owner = hosted(1);
    let registrar = owner.registrar();
    let (_, witness) = registrar
        .spawn_tracked(TaskClass::Auxiliary, async { Err(failure()) })
        .unwrap();
    let receipt = next(&mut owner).await;
    let local_record = Mutex::new(Some(witness.wait().await));
    assert!(local_record
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .result()
        .is_err());
    drop(local_record);
    drop(witness);
    assert!(!receipt.local_failure_accepted());
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    // The test now explicitly receives the original source on behalf of the
    // actual shutdown caller; this is independent of protocol-local acceptance.
    let retained = receipt.result().unwrap_err();
    owner.acknowledge(&receipt).unwrap();
    assert_eq!(retained.reason, Reason::Io);
    owner.begin_shutdown();
    assert!(owner.is_drained());
}
