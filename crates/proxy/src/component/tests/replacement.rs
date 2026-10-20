use super::*;
use crate::component::catalog::supervision::testing::TestContext;
use std::error::Error;
use tokio::{
    io::AsyncWriteExt,
    time::{Duration, Instant},
};
use zero_core::component::ResourceGeneration;
use zero_platform_tokio::contract::lifecycle::TaskClass;
#[path = "replacement/generations.rs"]
mod generations;
#[path = "replacement/native.rs"]
mod native;
#[path = "replacement/publication.rs"]
mod publication;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn config() -> Arc<RuntimeConfig> {
    Arc::new(RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap())
}
fn plan(slots: Vec<Arc<ComponentSlot>>) -> Arc<ComponentPlan> {
    Arc::new(ComponentPlan {
        builtin: slots[0].clone(),
        outbounds: BTreeMap::new(),
        order: slots,
        started: AtomicBool::new(false),
        deferred_devices: std::collections::BTreeSet::new(),
        device_pass: false,
        failures: Default::default(),
        shared_identities: Default::default(),
        confirmed_declared_close: Default::default(),
        closing: Default::default(),
        close_evidence: Default::default(),
    })
}
fn original_failure(error: &ChannelError) -> &Arc<plan::PlanTaskFailure> {
    error
        .source()
        .unwrap()
        .downcast_ref::<Arc<plan::PlanTaskFailure>>()
        .unwrap()
}

#[tokio::test]
async fn cancelled_task_retirement_keeps_the_same_batch_real_scope_and_original_join_failure() {
    let catalog = Arc::new(ComponentCatalog::default());
    let (blocked, gate) = native::task_slot(&catalog, false).await;
    let (panicking, panic) = native::task_slot(&catalog, true).await;
    let old = plan(vec![blocked.clone(), panicking.clone()]);
    TestContext::new(old.order.iter().map(AsRef::as_ref))
        .drive(old.start(deadline()))
        .await
        .unwrap();
    let mut peer = panic.peer.lock().unwrap().take().unwrap();
    peer.write_u8(1).await.unwrap();
    drop(peer);
    catalog.state.lock().unwrap().retired = Some(old.clone());
    let target = config();
    let next = catalog.prepare(target.clone()).unwrap();
    let first = tokio::spawn({
        let catalog = catalog.clone();
        let target = target.clone();
        async move { catalog.start(target, deadline()).await }
    });
    gate.pending.notified().await;
    let batch = catalog.state.lock().unwrap().replacement.clone().unwrap();
    assert!(Arc::ptr_eq(&batch.candidate, &next));
    assert!(old.confirmed(&BTreeSet::from([panicking.instance_id()])));
    assert!(!old.confirmed(&BTreeSet::from([blocked.instance_id()])));
    let failure = old
        .failures
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(failure.0.id, panic.task.lock().unwrap().unwrap());
    assert_eq!(failure.0.class, TaskClass::Critical);
    let native_id = panic.native_task.lock().unwrap().unwrap();
    let join = crate::component::tests::source::<tokio::task::JoinError>(&failure.0.error);
    assert!(join.is_panic());
    assert_eq!(join.id(), native_id);
    let source_address = std::ptr::from_ref(join);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(Arc::ptr_eq(
        &batch,
        catalog.state.lock().unwrap().replacement.as_ref().unwrap()
    ));
    drop(gate.peer.lock().unwrap().take());
    // Production has no real publication hook yet: completion cannot erase the fault.
    for _ in 0..2 {
        let error = catalog.start(target.clone(), deadline()).await.unwrap_err();
        assert!(Arc::ptr_eq(original_failure(&error), &failure));
        assert!(!batch.released());
    }
    assert!(old.confirmed(&BTreeSet::from([
        blocked.instance_id(),
        panicking.instance_id()
    ])));
    let mut supervisor = Vec::new();
    let reported = catalog
        .start_reported(target.clone(), deadline(), |terminal| {
            assert_eq!(terminal.instances().len(), 2);
            supervisor.extend(terminal.failures().iter().cloned());
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(batch.released());
    assert!(Arc::ptr_eq(&supervisor[0], &failure));
    assert!(Arc::ptr_eq(original_failure(&reported), &failure));
    let retained_join =
        crate::component::tests::source::<tokio::task::JoinError>(&supervisor[0].0.error);
    assert_eq!(std::ptr::from_ref(retained_join), source_address);
    // This rejects the failed attempt; only the next explicit retry starts target.
    assert!(!next.started.load(Ordering::Acquire));
    // Restore a different actual target. The same already-published task fault
    // must not be republished simply because a new retirement batch collects it.
    let restore = Arc::new(RuntimeConfig::parse(r#"{"outbounds":[{"tag":"restore","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap());
    catalog.start(restore.clone(), deadline()).await.unwrap();
    catalog.commit(&restore, || Ok(())).unwrap();
    assert!(
        old.shutdown(deadline()).await.is_err(),
        "raw plan shutdown remains sticky after handoff"
    );
    catalog.shutdown(deadline()).await.unwrap();
}

#[tokio::test]
async fn typed_receipt_reissues_after_unaccepted_publication_error_but_cannot_replay_consumption() {
    let catalog = ComponentCatalog::default();
    let (panicking, state) = native::task_slot(&catalog, true).await;
    let old = plan(vec![panicking]);
    let host = TestContext::new(old.order.iter().map(AsRef::as_ref));
    host.drive(old.start(deadline())).await.unwrap();
    let mut peer = state.peer.lock().unwrap().take().unwrap();
    peer.write_u8(1).await.unwrap();
    drop(peer);
    let target = catalog.prepare(config()).unwrap();
    let batch = Replacement::new(target, None, [old.clone()]).unwrap();
    let receipt = host.drive(batch.confirm(deadline())).await.unwrap();
    let failure = receipt.0.failure().unwrap().clone();
    let original_nonce = Arc::as_ptr(&receipt.0);
    assert!(batch
        .release(receipt, |_| Err(invalid(
            Reason::Internal,
            "publication failed"
        )))
        .is_err());
    assert!(!batch.released());
    let receipt = batch.confirm(deadline()).await.unwrap();
    assert_eq!(Arc::as_ptr(&receipt.0), original_nonce);
    let replay = batch.confirm(deadline()).await.unwrap();
    let foreign = Replacement::new(batch.candidate.clone(), None, []).unwrap();
    let foreign_receipt = foreign.confirm(deadline()).await.unwrap();
    let error = batch
        .release(foreign_receipt, |_| {
            panic!("foreign terminal cannot publish")
        })
        .unwrap_err();
    assert_eq!(error.reason, Reason::StalePlan);
    assert!(!batch.released());
    let mut supervisor = Vec::new();
    let error = batch
        .release(receipt, |terminal| {
            supervisor.extend(terminal.failures().iter().cloned());
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&supervisor[0], &failure));
    assert!(Arc::ptr_eq(original_failure(&error), &failure));
    let replayed = batch
        .release(replay, |_| panic!("consumed receipt cannot publish twice"))
        .unwrap_err();
    assert_eq!(replayed.reason, Reason::StalePlan);
    assert!(batch.released());
    assert!(Arc::ptr_eq(
        original_failure(&old.shutdown(deadline()).await.unwrap_err()),
        &failure
    ));
    let weak = Arc::downgrade(&old);
    drop(batch);
    drop(host);
    drop(old);
    assert!(
        weak.upgrade().is_none(),
        "published original task failures cannot retain the old plan"
    );
    assert!(
        crate::component::tests::source::<tokio::task::JoinError>(&supervisor[0].0.error)
            .is_panic()
    );
}

#[tokio::test]
async fn native_declaration_retains_actual_close_receipts_and_scope_fault_without_provider_proof() {
    let catalog = ComponentCatalog::default();
    let (slot, state) = native::native_slot(&catalog, true).await;
    let original = plan(vec![slot.clone()]);
    TestContext::new(original.order.iter().map(AsRef::as_ref))
        .drive(original.start(deadline()))
        .await
        .unwrap();
    native::assert_bound(&state);
    let mut peer = state.peer.lock().unwrap().take().unwrap();
    peer.write_u8(1).await.unwrap();
    drop(peer);
    catalog.state.lock().unwrap().retired = Some(original.clone());
    let target = config();
    let candidate = catalog.prepare(target.clone()).unwrap();
    for _ in 0..2 {
        let error = catalog.start(target.clone(), deadline()).await.unwrap_err();
        assert_eq!(error.reason, Reason::Unsupported);
        assert_eq!(
            error.delivery,
            zero_platform_tokio::contract::DeliveryState::Unknown
        );
        native::assert_released(&state);
        assert!(!original.confirmed(&BTreeSet::from([slot.instance_id()])));
        let batch = catalog.state.lock().unwrap().replacement.clone().unwrap();
        assert!(!batch.released());
        assert!(batch.terminal.get().is_none());
        let evidence = original.close_evidence.lock().unwrap();
        let retained = &evidence[&slot.instance_id()];
        assert_eq!(
            retained.closed_io.len(),
            1,
            "actual controlled original listener receipt survives retry"
        );
        assert_eq!(retained.exited_tasks, [state.task.lock().unwrap().unwrap()]);
        assert!(!retained.unfinished.is_empty());
    }
    let failure = original
        .failures
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(failure.0.class, TaskClass::Critical);
    assert!(!failure.reported());
    assert!(crate::component::tests::source::<tokio::task::JoinError>(&failure.0.error).is_panic());
    assert!(!candidate.started.load(Ordering::Acquire));
    assert!(catalog.commit(&target, || Ok(())).is_err());
    assert!(catalog.shutdown(deadline()).await.is_err());
    native::assert_released(&state);
}
