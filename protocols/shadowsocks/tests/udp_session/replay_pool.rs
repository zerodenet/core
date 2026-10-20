use super::*;
#[tokio::test(start_paused = true)]
async fn pool_capacity_never_evicts_live_salts_and_idle_worker_releases_memory() {
    let pool = ReplaySaltPool::with_limits(Duration::from_secs(61), 2);
    pool.check_and_insert(&[1; 16]).unwrap();
    pool.check_and_insert(&[2; 32]).unwrap();
    assert!(pool.check_and_insert(&[3; 16]).is_err());
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;
    assert!(pool.check_and_insert(&[1; 16]).is_err());
    // No insertion triggers cleanup: drive the worker's next deadline.
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    {
        let entries = pool.inner.lock().unwrap();
        assert!(entries.salts.is_empty());
        assert_eq!(entries.salts.capacity(), 0);
    }
    pool.check_and_insert(&[3; 16]).unwrap();
    let task = pool
        .maintenance
        .lock()
        .unwrap()
        .legacy
        .as_ref()
        .unwrap()
        .task
        .abort_handle();
    drop(pool);
    tokio::task::yield_now().await;
    assert!(task.is_finished());
}

#[tokio::test(start_paused = true)]
async fn registered_idle_driver_keeps_the_security_window_and_confirms_its_exit() {
    use std::num::NonZeroUsize;
    use zero_core::component::ComponentId;
    use zero_platform_tokio::contract::{
        lifecycle::TaskClass, scope::TaskScope, CancellationToken,
    };

    let pool = Arc::new(ReplaySaltPool::with_limits(Duration::from_secs(61), 1));
    pool.check_and_insert(&[1; 16]).unwrap();
    let legacy = pool.claim_maintenance(None).unwrap().unwrap();
    legacy.await.unwrap();
    assert!(
        pool.claim_maintenance(None).is_err(),
        "one owner per shared pool"
    );

    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let registrar = scope.registrar();
    let cancel = CancellationToken::new();
    registrar
        .spawn(
            TaskClass::Critical,
            pool.registered_maintenance(ComponentId(44), cancel.clone(), registrar.cancellation()),
        )
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;
    assert_eq!(pool.inner.lock().unwrap().salts.len(), 1);
    assert!(pool.check_and_insert(&[2; 16]).is_err());
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    {
        let entries = pool.inner.lock().unwrap();
        assert!(entries.salts.is_empty());
        assert_eq!(entries.salts.capacity(), 0);
    }
    pool.check_and_insert(&[2; 16]).unwrap();
    assert!(pool.maintenance.lock().unwrap().legacy.is_none());
    cancel.cancel();
    let report = scope
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 1);
    assert!(report.failed_tasks.is_empty());
    pool.release_maintenance(None);
    assert!(pool.check_and_insert(&[2; 16]).is_err());
    assert!(pool.claim_maintenance(None).unwrap().is_none());
    pool.release_maintenance(None);
}
