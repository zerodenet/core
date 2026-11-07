//! An accepted configuration job never waits for its own command permission.
use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_actual_apply_can_retire_raw_scope_while_its_frontend_credit_remains_live() {
    timeout(Duration::from_secs(5), async {
        let book = book();
        let owner = book.command_owner();
        let row = owner
            .adopt_command(book.reserve(ControlCharge::PendingCommand).unwrap())
            .unwrap();
        let old_scope = lock(&owner.state)
            .tasks
            .as_ref()
            .unwrap()
            .retirement_observation();
        let mut frontend =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        frontend.enable_registration().unwrap();
        let apply = Arc::new(crate::ApplyOwner::default());
        let observed = row.observation();
        let original_observation = observed.clone();
        let actual_book = book.clone();
        let actual_apply = apply.clone();
        let actual_row = row.clone();
        let (start, gate) = oneshot::channel();
        let (_, worker) = frontend
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                gate.await.unwrap();
                let serial = Arc::new(tokio::sync::Mutex::new(()));
                observed
                    .scope(actual_apply.run(serial, async move {
                        let mut limits = actual_book.limits();
                        limits.messages.pending_commands = 2;
                        let candidate = actual_book.prepare(limits).map_err(|e| e.to_string())?;
                        candidate
                            .retire_command_scope(Instant::now() + Duration::from_secs(2))
                            .await
                            .map_err(|e| e.to_string())?;
                        assert_eq!(
                            actual_book.usage().pending_commands,
                            1,
                            "the accepted Apply still owns its actual frontend permission"
                        );
                        candidate.commit().map_err(|e| e.to_string())?;
                        assert_eq!(actual_book.limits().messages.pending_commands, 2);
                        Ok(())
                    }))
                    .await
                    .map_err(|e| fault(Reason::Internal).with_source(std::io::Error::other(e)))?;
                actual_row.finish_inline().await?;
                Ok(())
            })
            .unwrap();
        assert!(row.register_scoped(worker.clone()));
        start.send(()).unwrap();
        let receipt = frontend.next_receipt().await.unwrap();
        assert!(worker.matches_joined(receipt.joined()));
        assert!(receipt.result().is_ok());
        assert!(
            old_scope.is_drained(),
            "only the old raw-command duty actually retired"
        );
        let joined = original_observation.wait_joined().await.unwrap();
        assert!(joined.registration_released() && joined.result().is_ok());
        assert_eq!(
            book.usage().pending_commands,
            1,
            "Apply ACK cannot replace the original frontend task ACK"
        );
        assert!(!receipt.joined().registration_released());
        frontend.acknowledge(&receipt).unwrap();
        loop {
            if book.usage().pending_commands == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(owner.retirement_complete());
        frontend.begin_shutdown();
        assert!(frontend.next_receipt().await.is_none() && frontend.is_drained());
        apply
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_frontend_cannot_enter_raw_acquisition_after_same_candidate_fence() {
    timeout(Duration::from_secs(5), async {
        let book = book();
        let owner = book.command_owner();
        let row = owner
            .adopt_command(book.reserve(ControlCharge::PendingCommand).unwrap())
            .unwrap();
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let (start, gate) = oneshot::channel();
        let (denied, observed) = oneshot::channel();
        let entered = Arc::new(AtomicBool::new(false));
        let marker = entered.clone();
        let original = row.clone();
        let (_, caller) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                gate.await.unwrap();
                let observation = original.observation();
                observation
                    .scope(async {
                        let mut raw = Box::pin(original.run_blocking(
                            move || {
                                marker.store(true, Ordering::Release);
                            },
                            |_| async { Ok(()) },
                        ));
                        std::future::poll_fn(|cx| {
                            use std::future::Future;
                            use std::task::Poll;
                            match raw.as_mut().poll(cx) {
                                Poll::Ready(Err(error)) => {
                                    assert_eq!(error.reason, Reason::Revoked);
                                    Poll::Ready(())
                                }
                                Poll::Ready(Ok(())) => {
                                    panic!("fenced actual raw work was accepted")
                                }
                                Poll::Pending => panic!(
                                    "fenced acquisition waited for the retiring Scope serial"
                                ),
                            }
                        })
                        .await;
                    })
                    .await;
                assert!(original.test_raw_witness().is_none());
                original.finish_inline().await?;
                denied.send(()).unwrap();
                Ok(())
            })
            .unwrap();
        assert!(row.register_scoped(caller.clone()));
        let mut limits = book.limits();
        limits.messages.pending_commands = 2;
        let candidate = book.prepare(limits).unwrap();
        let serial = owner.serial.lock().await;
        start.send(()).unwrap();
        observed.await.unwrap();
        assert!(!entered.load(Ordering::Acquire));
        assert_eq!(book.usage().pending_commands, 1);
        drop(serial);
        candidate
            .retire_command_scope(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        candidate.commit().unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(caller.matches_joined(receipt.joined()) && receipt.result().is_ok());
        assert!(!receipt.joined().registration_released());
        assert_eq!(book.usage().pending_commands, 1);
        scope.acknowledge(&receipt).unwrap();
        loop {
            if book.usage().pending_commands == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(owner.retirement_complete());
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none() && scope.is_drained());
    })
    .await
    .unwrap();
}
