use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn opener(
    opened: Arc<AtomicUsize>,
) -> impl Clone + Fn() -> std::future::Ready<Result<TcpRelayStream, RuntimeError>> {
    move || {
        opened.fetch_add(1, Ordering::SeqCst);
        let (stream, _peer) = tokio::io::duplex(64);
        std::future::ready(Ok(TcpRelayStream::new(stream)))
    }
}

#[test]
fn registry_reuses_pool_for_the_same_outbound_identity() {
    let registry = Registry::default();
    let identity = [1; 32];
    let first = registry.create("out", identity, 2).unwrap();
    let second = registry.create("out", identity, 2).unwrap();
    let other = registry.create("other", identity, 2).unwrap();
    let changed_carrier = registry.create("out", [2; 32], 2).unwrap();
    let changed_capacity = registry.create("out", identity, 3).unwrap();

    assert!(Arc::ptr_eq(&first.0, &second.0));
    assert!(!Arc::ptr_eq(&first.0, &other.0));
    assert!(!Arc::ptr_eq(&first.0, &changed_carrier.0));
    assert!(!Arc::ptr_eq(&first.0, &changed_capacity.0));
}

#[test]
fn registry_owns_pool_until_reload() {
    let registry = Registry::default();
    let pool = registry.create("out", [1; 32], 1).unwrap();
    let retained = Arc::downgrade(&pool.0);

    drop(pool);
    assert!(retained.upgrade().is_some());
    registry.retire();
    assert!(retained.upgrade().is_none());
}

#[tokio::test]
async fn configured_worker_count_opens_that_many_ready_connections() {
    let registry = Registry::default();
    let pool = registry.create("out", [1; 32], 3).unwrap();
    let opened = Arc::new(AtomicUsize::new(0));

    drop(pool.take(opener(opened.clone())).await.unwrap());
    for _ in 0..10 {
        if opened.load(Ordering::SeqCst) == 3 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(opened.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn expired_connection_is_discarded_and_replenished() {
    let registry = Registry::default();
    let pool = registry.create("out", [1; 32], 1).unwrap();
    let opened = Arc::new(AtomicUsize::new(0));

    let first = pool.take(opener(opened.clone())).await.unwrap();
    drop(first);
    tokio::task::yield_now().await;
    tokio::time::advance(REPLENISH_DELAY + Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(opened.load(Ordering::SeqCst), 2);

    tokio::time::advance(CONNECTION_LIFETIME + Duration::from_secs(1)).await;
    let replacement = pool.take(opener(opened.clone())).await.unwrap();
    drop(replacement);
    assert_eq!(opened.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn replenishment_delay_starts_after_a_queued_connection_is_taken() {
    let registry = Registry::default();
    let pool = registry.create("out", [1; 32], 1).unwrap();
    let opened = Arc::new(AtomicUsize::new(0));
    pool.0.start(opener(opened.clone()));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(opened.load(Ordering::SeqCst), 1);

    drop(pool.take(opener(opened.clone())).await.unwrap());
    tokio::task::yield_now().await;
    tokio::time::advance(REPLENISH_DELAY - Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(opened.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(opened.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn retiring_registry_stops_replenishment_and_releases_waiters() {
    let registry = Registry::default();
    let pool = registry.create("out", [1; 32], 1).unwrap();
    let opened = Arc::new(AtomicUsize::new(0));

    drop(pool.take(opener(opened.clone())).await.unwrap());
    registry.retire();
    assert!(pool.take(opener(opened.clone())).await.is_err());
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(opened.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_last_registry_owner_retires_pending_pool_without_restarting_workers() {
    let registry = Registry::default();
    let pool = registry.create("out", [1; 32], 1).unwrap();
    let opened = Arc::new(AtomicUsize::new(0));
    let pending_opened = opened.clone();
    let waiting_pool = pool.clone();
    let waiter = tokio::spawn(async move {
        waiting_pool
            .take(move || {
                pending_opened.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<TcpRelayStream, RuntimeError>>()
            })
            .await
    });

    for _ in 0..10 {
        if opened.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(opened.load(Ordering::SeqCst), 1);

    drop(registry);
    assert!(waiter.await.unwrap().is_err());
    assert!(pool.take(opener(opened.clone())).await.is_err());
    tokio::task::yield_now().await;
    assert_eq!(opened.load(Ordering::SeqCst), 1);
}
