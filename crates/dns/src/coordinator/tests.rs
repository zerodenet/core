use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn key(generation: u64) -> QueryKey {
    QueryKey::new("storm.example", 1, QueryScope::new(DnsQueryRole::Direct, generation))
}

#[tokio::test]
async fn coalesces_identical_concurrent_queries() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let coordinator = coordinator.clone();
        let calls = Arc::clone(&calls);
        tasks.push(tokio::spawn(async move {
            coordinator
                .resolve(key(7), async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    Ok(vec![IpAddress::V4([192, 0, 2, 1])])
                })
                .await
        }));
    }
    for task in tasks {
        assert_eq!(
            task.await.unwrap().unwrap(),
            vec![IpAddress::V4([192, 0, 2, 1])]
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn negative_cache_suppresses_a_failure_burst() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let calls = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let calls = Arc::clone(&calls);
        let error = coordinator
            .resolve(key(7), async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::new(io::ErrorKind::TimedOut, "backend timeout"))
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn topology_generation_does_not_reuse_old_negative_results() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    coordinator
        .resolve(key(7), async {
            Err(io::Error::new(io::ErrorKind::NotConnected, "old route"))
        })
        .await
        .unwrap_err();

    let resolved = coordinator
        .resolve(key(8), async { Ok(vec![IpAddress::V4([198, 51, 100, 2])]) })
        .await
        .unwrap();
    assert_eq!(resolved, vec![IpAddress::V4([198, 51, 100, 2])]);
}

#[tokio::test]
async fn topology_generation_cancels_old_in_flight_work_and_wakes_waiters() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let old = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .resolve(key(7), std::future::pending())
                .await
                .expect_err("old DNS flight must be cancelled")
        })
    };
    tokio::task::yield_now().await;

    let current = coordinator
        .resolve(key(8), async { Ok(vec![IpAddress::V4([203, 0, 113, 8])]) })
        .await
        .unwrap();
    assert_eq!(current, vec![IpAddress::V4([203, 0, 113, 8])]);
    let error = tokio::time::timeout(Duration::from_secs(1), old)
        .await
        .expect("old waiter must wake promptly")
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::NotConnected);
}

#[tokio::test]
async fn old_egress_snapshot_cannot_cancel_a_newer_flight() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let (entered, received) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let current = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator.resolve(key(8), async move {
                let _ = entered.send(());
                let _ = released.await;
                Ok(vec![IpAddress::V4([203, 0, 113, 8])])
            }).await
        })
    };
    received.await.unwrap();
    let stale = coordinator.resolve(key(7), async { Ok(Vec::new()) }).await.unwrap_err();
    assert_eq!(stale.kind(), io::ErrorKind::NotConnected);
    let _ = release.send(());
    assert_eq!(current.await.unwrap().unwrap(), vec![IpAddress::V4([203, 0, 113, 8])]);
}

#[tokio::test]
async fn negative_results_are_scoped_by_family_and_config_generation() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let mut old_key = key(7);
    old_key.scope.family = AddressFamily::OnlyIpv4;
    coordinator.resolve(old_key.clone(), async {
        Err(io::Error::new(io::ErrorKind::NotFound, "old policy failure"))
    }).await.unwrap_err();

    let mut auto_key = old_key.clone();
    auto_key.scope.family = AddressFamily::Auto;
    assert_eq!(coordinator.resolve(auto_key, async { Ok(vec![IpAddress::V4([192, 0, 2, 1])]) }).await.unwrap(), vec![IpAddress::V4([192, 0, 2, 1])]);
    let mut current_key = old_key;
    current_key.scope.config_generation += 1;
    assert_eq!(coordinator.resolve(current_key, async { Ok(vec![IpAddress::V4([192, 0, 2, 2])]) }).await.unwrap(), vec![IpAddress::V4([192, 0, 2, 2])]);
}
