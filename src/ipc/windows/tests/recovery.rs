//! A retired original fault remains observable while the same namespace recovers.
use super::*;
#[tokio::test]
async fn named_pipe_fault_retains_original_error_and_recovers_after_native_and_scope_close() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (_, handle) = handles();
        let budget = budget();
        let path = path();
        let old = prepare_ipc_server_with_budget(&path, budget.clone());
        old.start(handle.clone()).await.unwrap();
        let client = ping(&path).await;
        let originals = actual_pending(&old).await;
        // Complete the second native connection synchronously before closing
        // admission; this already charged original wakes the actual accept loop.
        let triggering = ClientOptions::new().open(&path).unwrap();
        budget.close_admission();
        let fault = old.wait_terminated().await.unwrap_err();
        assert!(!old.retirement_complete());
        let retired = old.shutdown().await.unwrap_err();
        assert!(old.retirement_complete() && old.originals.is_empty());
        let original = fault
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        let returned = retired
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        assert!(Arc::ptr_eq(original, returned));
        originals_closed(&originals);
        budget.confirm_released().unwrap();
        drop(triggering);
        drop(client);
        let fresh_budget = super::budget();
        let fresh = prepare_ipc_server_with_budget(&path, fresh_budget.clone());
        fresh.start(handle).await.unwrap();
        let client = ping(&path).await;
        let successor = actual_pending(&fresh).await;
        assert!(successor.iter().all(|entry| originals.iter().all(|prior| {
            !entry
                .original
                .matches_closed(&prior.original.closed().unwrap())
        })));
        fresh.shutdown().await.unwrap();
        assert!(fresh.retirement_complete());
        originals_closed(&successor);
        fresh_budget.close_admission();
        fresh_budget.confirm_released().unwrap();
        drop(client);
    })
    .await
    .unwrap();
}
