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
    let task = pool.maintenance.lock().unwrap().as_ref().unwrap().clone();
    drop(pool);
    tokio::task::yield_now().await;
    assert!(task.is_finished());
}
