use super::*;

#[tokio::test]
async fn cancelling_one_waiter_keeps_the_shared_worker_and_its_remaining_caller() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let first = {
        let coordinator = coordinator.clone();
        let entered = entered.clone();
        let release = release.clone();
        tokio::spawn(async move {
            coordinator
                .resolve(key(7), async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(vec![IpAddress::V4([192, 0, 2, 5])])
                })
                .await
        })
    };
    entered.notified().await;
    let second = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .resolve(key(7), async {
                    panic!("follower must not execute another backend")
                })
                .await
        })
    };
    loop {
        let waiters = coordinator.state.lock().unwrap().in_flight[&key(7)]
            .waiters
            .load(Ordering::Acquire);
        if waiters == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    release.notify_one();
    assert_eq!(
        second.await.unwrap().unwrap(),
        vec![IpAddress::V4([192, 0, 2, 5])]
    );
    coordinator
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn last_waiter_cancellation_retires_and_actually_joins_its_worker_without_caching_a_failure()
{
    struct Exit(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for Exit {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let (entered, received) = tokio::sync::oneshot::channel();
    let (exited, exit) = tokio::sync::oneshot::channel();
    let request = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .resolve(key(7), async move {
                    let _exit = Exit(Some(exited));
                    let _ = entered.send(());
                    std::future::pending().await
                })
                .await
        })
    };
    received.await.unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), exit)
        .await
        .unwrap()
        .unwrap();
    assert!(coordinator.state.lock().unwrap().in_flight.is_empty());
    assert!(coordinator.state.lock().unwrap().failures.is_empty());
    assert_eq!(
        coordinator
            .resolve(key(7), async { Ok(vec![IpAddress::V4([203, 0, 113, 1])]) })
            .await
            .unwrap(),
        vec![IpAddress::V4([203, 0, 113, 1])]
    );
    coordinator
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(coordinator.state.lock().unwrap().tasks.is_empty());
}
