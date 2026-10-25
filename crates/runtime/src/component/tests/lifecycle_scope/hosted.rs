use super::*;

#[tokio::test]
async fn auxiliary_delivery_keeps_capacity_until_local_persistence_and_has_no_success_history() {
    use crate::component::catalog::supervision::testing::TestContext;
    use zero_platform_tokio::contract::scope::SpawnError;
    timeout(Duration::from_secs(3), async {
        let (slot, state) = fixture_with(TaskClass::Auxiliary, false);
        let host = TestContext::new([&slot]);
        // The nested real work progresses even while its outer driver owns the
        // single lease. No actor or background join task is created here.
        host.drive(host.drive(slot.start(Instant::now() + Duration::from_secs(2))))
            .await
            .unwrap();
        state.task_pending.notified().await;
        let witness = state.witness.lock().unwrap().clone().unwrap();
        // Use the actual registration issued to this lifecycle; no new Scope.
        let registrar = slot
            .live
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .tasks
            .registrar_for_test();
        let mut peer = state.task_peer.lock().unwrap().take().unwrap();
        peer.write_u8(1).await.unwrap();
        let joined = host.drive(witness.wait()).await;
        let original = joined.result().unwrap_err();
        assert_eq!(joined.class(), TaskClass::Auxiliary);
        assert_eq!(
            registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        assert!(
            slot.task_fault().is_none(),
            "Auxiliary is not a component Critical fault"
        );
        assert!(!slot.cancel.is_cancelled());
        // This is the explicit local receiver's bounded, same-error record.
        // Mere witness observation above did not acknowledge anything.
        let local_record = (joined.clone(), original.clone());
        witness.acknowledge_hosted_failure(&local_record.0).unwrap();
        host.drive(slot.scan_tasks(Instant::now() + Duration::from_secs(1)))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&local_record.1, &joined.result().unwrap_err()));
        for _ in 0..32 {
            let (mut io, mut peer) = tokio::io::duplex(1);
            let (id, witness) = registrar
                .spawn_tracked(TaskClass::Auxiliary, async move {
                    io.read_u8().await.unwrap();
                    Ok(())
                })
                .unwrap();
            peer.write_u8(1).await.unwrap();
            let closed = host.drive(witness.wait()).await;
            assert_eq!(closed.id(), id);
            closed.result().unwrap();
        }
        let report = host
            .drive(slot.shutdown(Instant::now() + Duration::from_secs(1)))
            .await
            .unwrap();
        assert!(report.is_complete());
        assert!(
            report.exited_tasks.is_empty(),
            "delivered successes are not a lifetime task-ID log"
        );
        assert!(
            report.failed_tasks.is_empty(),
            "the real local receiver owns its Auxiliary failure"
        );
        assert!(!slot.can_reuse());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelling_the_active_inline_driver_leaves_same_scope_for_actual_cleanup_takeover() {
    use crate::component::catalog::supervision::testing::TestContext;
    timeout(Duration::from_secs(3), async {
        let (slot, state) = fixture();
        let host = TestContext::new([&slot]);
        let mut starting =
            Box::pin(host.drive(slot.start(Instant::now() + Duration::from_secs(30))));
        tokio::select! {
            result = starting.as_mut() => panic!("start did not suspend: {result:?}"),
            _ = state.start_pending.notified() => {},
        }
        state.task_pending.notified().await;
        let witness = state.witness.lock().unwrap().clone().unwrap();
        assert!(witness.try_joined().is_none());
        drop(starting);
        let report = host
            .drive(slot.shutdown(Instant::now() + Duration::from_secs(1)))
            .await
            .unwrap();
        let joined = witness.wait().await;
        assert!(report.is_complete());
        assert_eq!(report.exited_tasks, [joined.id()]);
        assert!(report.failed_tasks.is_empty());
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}
