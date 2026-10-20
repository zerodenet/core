use super::*;

#[tokio::test]
async fn successful_shutdown_prevents_a_preselected_worker_from_registering_after_confirmation() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let (entered, selected) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    coordinator.state.lock().unwrap().spawn_gate = Some(SpawnGate {
        entered,
        resume: resumed,
    });
    let started = Arc::new(AtomicUsize::new(0));
    let caller = {
        let coordinator = coordinator.clone();
        let started = started.clone();
        tokio::spawn(async move {
            coordinator
                .resolve(key(7), async move {
                    started.fetch_add(1, Ordering::AcqRel);
                    Ok(vec![])
                })
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), selected)
        .await
        .unwrap()
        .unwrap();
    {
        let state = coordinator.state.lock().unwrap();
        assert_eq!(
            state.in_flight.len(),
            1,
            "actual query flight is already selected"
        );
        assert!(
            state.tasks.is_empty(),
            "worker is paused before final task admission"
        );
    }
    let follower = {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .resolve(key(7), async {
                    panic!("a coalesced follower cannot run a second backend")
                })
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if coordinator.state.lock().unwrap().in_flight[&key(7)]
                .waiters
                .load(Ordering::Acquire)
                == 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("empty task set must not starve the real coordinator owner");
    coordinator
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(coordinator.state.lock().unwrap().tasks.is_empty());
    let error = tokio::time::timeout(Duration::from_secs(1), follower)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotConnected);
    resume.send(()).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), caller)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotConnected);
    let state = coordinator.state.lock().unwrap();
    assert!(state.closing);
    assert!(state.in_flight.is_empty());
    assert!(
        state.tasks.is_empty(),
        "successful shutdown must not acquire an unjoined worker later"
    );
    assert_eq!(started.load(Ordering::Acquire), 0);
}
