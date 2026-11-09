//! Fault acceptance never substitutes for original native/view/Scope closure.
use super::*;
use zero_platform_tokio::contract::lifecycle::ResourceFailurePhase;
#[path = "resource_fault_fixture.rs"]
mod fixture;

async fn start(
    catalog: &ComponentCatalog,
    unknown: bool,
) -> (Arc<ComponentPlan>, Arc<fixture::State>) {
    let (slot, state) = fixture::slot(catalog, "127.0.0.1:0".parse().unwrap(), unknown);
    let old = plan(vec![slot]);
    let host = TestContext::new(old.order.iter().map(AsRef::as_ref));
    host.drive(old.start(deadline())).await.unwrap();
    host.drive(state.pending.notified()).await;
    assert!(!state.witness.lock().unwrap().as_ref().unwrap().is_joined());
    (old, state)
}
fn canonical(old: &ComponentPlan, state: &fixture::State) -> Arc<plan::PlanFailure> {
    let failures = old.failures.lock().unwrap();
    assert_eq!(
        failures.len(),
        1,
        "one actual controller, no synthetic TaskId"
    );
    let actual = failures.values().next().unwrap().clone();
    assert!(actual.task().is_none());
    assert_eq!(actual.instance(), old.order[0].instance_id());
    assert_eq!(
        actual.key(),
        plan::FailureKey::Resource(actual.instance(), ResourceFailurePhase::ProtocolLifecycle)
    );
    assert!(Arc::ptr_eq(
        crate::component::tests::source::<Arc<ChannelError>>(actual.original()),
        &state.failure
    ));
    assert!(state
        .failure
        .cleanup_failure_receipt()
        .unwrap()
        .is_acknowledged());
    {
        let evidence = old.close_evidence.lock().unwrap();
        let report = &evidence[&old.order[0].instance_id()];
        let received = report
            .resource_failures
            .iter()
            .find(|failure| failure.controller == ResourceFailurePhase::ProtocolLifecycle)
            .unwrap();
        assert!(received.ownership_received(),
            "actual canonical archive must accept the RF and its original ledger, not only its receipt");
    }
    actual
}

#[tokio::test]
async fn actual_cleanup_deadline_is_archived_before_ack_and_same_original_late_join_allows_replacement(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let catalog = ComponentCatalog::default();
        let (old, state) = start(&catalog, false).await;
        let address = state.address.lock().unwrap().unwrap();
        let (next_slot, next_state) = fixture::slot(&catalog, address, false);
        let next = plan(vec![next_slot]);
        let batch = Replacement::new(next.clone(), None, [old.clone()]).unwrap();
        let host = TestContext::new(old.order.iter().chain(next.order.iter()).map(AsRef::as_ref));
        assert_eq!(Arc::strong_count(&state.failure), 1);
        {
            let first = host.drive(batch.confirm(deadline()));
            tokio::pin!(first);
            tokio::select! {
                _ = state.returned.notified() => {},
                _ = &mut first => panic!("the real driver cleanup is still pending"),
            }
        }
        assert!(
            !state
                .failure
                .cleanup_failure_receipt()
                .unwrap()
                .is_acknowledged(),
            "a cancelled observer cannot acknowledge an unarchived phase"
        );
        assert_eq!(
            Arc::strong_count(&state.failure),
            2,
            "the actual Owner keeps the first returned source after the cancelled join observer"
        );
        let error = host
            .drive(batch.confirm(Instant::now() + Duration::from_millis(40)))
            .await
            .err()
            .unwrap();
        let original = canonical(&old, &state);
        assert!(!original.reported(),
            "local original ownership never fabricates outer supervisor publication");
        assert!(Arc::ptr_eq(original_failure(&error), &original));
        assert!(!batch.released());
        assert!(!next.started.load(Ordering::Acquire));
        assert!(!old.confirmed(&BTreeSet::from([old.order[0].instance_id()])));
        let witness = state.witness.lock().unwrap().as_ref().unwrap().clone();
        assert!(
            !witness.is_joined(),
            "the original driver still owns its returned-view continuation"
        );
        let actual_provider_receipts = old.close_evidence.lock().unwrap();
        let actual_report = &actual_provider_receipts[&old.order[0].instance_id()];
        assert_eq!(actual_report.closed_io.len(), 1,
            "the actual single declared listener provider supplied its original native close receipt");
        drop(actual_provider_receipts);
        state
            .release
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        let receipt = host.drive(batch.confirm(deadline())).await.unwrap();
        let joined = witness.try_joined().unwrap();
        assert!(witness.matches_joined(&joined) && joined.registration_released());
        let mut receiver = Vec::new();
        let error = batch
            .release(receipt, |terminal| {
                receiver.extend(terminal.failures().iter().cloned());
                Ok(())
            })
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(original_failure(&error), &original));
        assert_eq!(receiver.len(), 1);
        assert!(Arc::ptr_eq(&receiver[0], &original) && original.reported());
        assert!(old.confirmed(&BTreeSet::from([old.order[0].instance_id()])));
        // The new actual native bind happens only after the positive original
        // provider, protocol, driver and hosted-ACK gates above.
        host.drive(next.start(deadline())).await.unwrap();
        assert_eq!(*next_state.address.lock().unwrap(), Some(address));
        host.drive(next_state.pending.notified()).await;
        next_state
            .release
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        let ids = BTreeSet::from([next.order[0].instance_id()]);
        let _ = host.drive(next.confirm_instances(&ids, deadline())).await;
        host.drive(next.confirm_instances(&ids, deadline()))
            .await
            .unwrap();
        assert!(next.confirmed(&ids));
        assert!(
            Arc::ptr_eq(&canonical(&old, &state), &original),
            "recovery cannot erase the published original fault"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn accepted_original_fault_and_actual_scope_ack_do_not_replace_unknown_protocol_close() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let catalog = ComponentCatalog::default();
        let (old, state) = start(&catalog, true).await;
        let candidate = catalog.prepare(config()).unwrap();
        let batch = Replacement::new(candidate.clone(), None, [old.clone()]).unwrap();
        let host = TestContext::new(old.order.iter().map(AsRef::as_ref));
        assert!(host
            .drive(batch.confirm(Instant::now() + Duration::from_millis(40)))
            .await
            .is_err());
        let original = canonical(&old, &state);
        state
            .release
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        for _ in 0..2 {
            let error = host.drive(batch.confirm(deadline())).await.err().unwrap();
            assert!(Arc::ptr_eq(original_failure(&error), &original));
            assert!(!batch.released() && !candidate.started.load(Ordering::Acquire));
        }
        let witness = state.witness.lock().unwrap().as_ref().unwrap().clone();
        let joined = witness.try_joined().unwrap();
        assert!(witness.matches_joined(&joined) && joined.registration_released());
        assert!(!old.confirmed(&BTreeSet::from([old.order[0].instance_id()])));
        assert!(
            !original.reported(),
            "no terminal may publish replacement closure for an unknown original"
        );
    })
    .await
    .unwrap();
}
