//! Ordinary Host handoff after real task joins, including a lost waiter.
use super::*;

#[tokio::test]
async fn actual_host_retains_closed_original_fault_after_cancelled_waiter_and_explicit_restore() {
    let host = Arc::new(crate::RuntimeHost::<()>::new(
        crate::component::tests::default_factories(),
    ));
    let catalog = host.components();
    let (blocked, gate) = native::task_slot(catalog, false).await;
    let (panicking, panic) = native::task_slot(catalog, true).await;
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
        let host = host.clone();
        let target = target.clone();
        async move { host.components().start(target, deadline()).await }
    });
    gate.pending.notified().await;
    let batch = catalog.state.lock().unwrap().replacement.clone().unwrap();
    assert!(!batch.released());
    assert!(host.apply().component_retirement().is_none());
    assert!(!next.started.load(Ordering::Acquire));
    assert!(!old.confirmed(&BTreeSet::from([blocked.instance_id()])));
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(host.apply().component_retirement().is_none());
    assert!(Arc::ptr_eq(
        catalog.state.lock().unwrap().replacement.as_ref().unwrap(),
        &batch
    ));
    // Return the exact pending read. Neither waiter cancellation nor body
    // completion can stand in for this Scope's actual join/close collection.
    drop(gate.peer.lock().unwrap().take());
    let failure = catalog.start(target.clone(), deadline()).await.unwrap_err();
    assert!(batch.released());
    assert!(!next.started.load(Ordering::Acquire));
    assert!(old.confirmed(&BTreeSet::from([
        blocked.instance_id(),
        panicking.instance_id()
    ])));
    let original = original_failure(&failure).clone();
    let retained = host.apply().component_retirement().unwrap();
    assert!(Arc::ptr_eq(&retained, batch.terminal.get().unwrap()));
    assert_eq!(retained.instances().len(), 2);
    assert_eq!(retained.failures().len(), 1);
    assert!(Arc::ptr_eq(&retained.failures()[0], &original));
    let native_id = panic.native_task.lock().unwrap().unwrap();
    let join = crate::component::tests::source::<tokio::task::JoinError>(&failure);
    assert!(join.is_panic());
    assert_eq!(join.id(), native_id);
    let address = std::ptr::from_ref(join);
    // The failed attempt returns the same typed source. Only the next explicit
    // operation may start and commit; it uses ordinary production entrypoints.
    catalog.start(target.clone(), deadline()).await.unwrap();
    catalog.commit(&target, || Ok(())).unwrap();
    assert!(next.started.load(Ordering::Acquire));
    assert!(Arc::ptr_eq(
        host.apply().component_retirement().as_ref().unwrap(),
        &retained
    ));
    assert_eq!(
        std::ptr::from_ref(crate::component::tests::source::<tokio::task::JoinError>(
            &retained.failures()[0].retained()
        )),
        address
    );
    catalog.shutdown(deadline()).await.unwrap();
}

#[tokio::test]
async fn lost_actual_configuration_owner_keeps_original_fault_unpublished_without_a_fallback() {
    let catalog = ComponentCatalog::default();
    let owner = Arc::new(crate::apply_owner::ApplyOwner::default());
    catalog.attach_configuration_owner(Arc::downgrade(&owner));
    drop(owner);
    let (panicking, panic) = native::task_slot(&catalog, true).await;
    let old = plan(vec![panicking]);
    TestContext::new(old.order.iter().map(AsRef::as_ref))
        .drive(old.start(deadline()))
        .await
        .unwrap();
    let mut peer = panic.peer.lock().unwrap().take().unwrap();
    peer.write_u8(1).await.unwrap();
    drop(peer);
    catalog.state.lock().unwrap().retired = Some(old);
    let target = config();
    let failure = catalog.start(target.clone(), deadline()).await.unwrap_err();
    let batch = catalog.state.lock().unwrap().replacement.clone().unwrap();
    assert!(!batch.released());
    let retry = catalog.start(target.clone(), deadline()).await.unwrap_err();
    assert!(Arc::ptr_eq(
        original_failure(&failure),
        original_failure(&retry)
    ));
    assert!(!batch.released());
    // Explicit genuine standalone supervision can still receive the original;
    // losing a Host never constructs one behind the caller's back.
    let mut received = Vec::new();
    catalog
        .start_reported(target.clone(), deadline(), |terminal| {
            received.extend(terminal.failures().iter().cloned());
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(Arc::ptr_eq(&received[0], original_failure(&failure)));
    catalog.start(target.clone(), deadline()).await.unwrap();
    catalog.commit(&target, || Ok(())).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
}
