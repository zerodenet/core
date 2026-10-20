use super::*;
use crate::runtime::listeners::reconcile_inbounds_with_before_bind;
use std::sync::atomic::AtomicBool;

#[tokio::test]
async fn before_bind_gets_the_original_terminal_only_after_pending_native_close_and_real_join() {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new(1).await;
        let port = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut candidate = (*fixture.old).clone();
        candidate.inbounds[0].listen.port = port.local_addr().unwrap().port();
        fixture.proxy.engine.stage_runtime_config(candidate).unwrap();
        let candidate = fixture.proxy.engine().config();
        let next = factory(&fixture.proxy);
        let rollback = fixture.rollback.clone();
        let protocols = fixture.proxy.protocols.clone();
        let pending = fixture.gate.pending.clone();
        let entered = Arc::new(AtomicBool::new(false));
        let first = entered.clone();
        let mut applying = Box::pin(reconcile_inbounds_with_before_bind(
            &protocols, None, &next, &rollback, &candidate, fixture.state(),
            move |_phase| async move { first.store(true, Ordering::Release); Ok(()) },
        ));
        tokio::select! {
            _ = pending.notified() => {},
            result = &mut applying => panic!("actual native cleanup must still be pending: {result:?}"),
        }
        assert!(!entered.load(Ordering::Acquire));
        drop(applying); // cancellation is not completion of the old physical owner
        assert!(!fixture.gate.closed.load(Ordering::Acquire));
        fixture.gate.release.write_u8(1).await.unwrap();
        drop(port);
        let original = fixture.originals[0].clone();
        let owners = fixture.proxy.inbound_owners.clone();
        let actual_config = candidate.clone();
        let wrong_config = Arc::new((*candidate).clone());
        let closed = fixture.gate.closed.clone();
        let second = entered.clone();
        reconcile_inbounds_with_before_bind(
            &protocols, None, &next, &rollback, &candidate, fixture.state(),
            move |phase| async move {
                assert!(closed.load(Ordering::Acquire));
                assert!(phase.matches_candidate(&actual_config));
                assert!(!phase.matches_candidate(&wrong_config));
                let terminal = phase.terminal("old-0").expect("original physical terminal");
                let original = original.terminal_for_test().unwrap();
                assert!(std::ptr::eq(terminal, original.as_ref()));
                assert!(terminal.joined_tasks() > 0);
                assert!(terminal.closed_io_receipts() >= 2);
                assert!(!phase.owner_was_absent("old-0"));
                assert!(owners.owner("old-0").is_none()); // before the first candidate bind
                second.store(true, Ordering::Release);
                Ok(())
            },
        ).await.unwrap();
        assert!(entered.load(Ordering::Acquire));
        ready_socks(candidate.inbounds[0].listen.port).await;
        fixture.finish().await;
    }).await.unwrap();
}
