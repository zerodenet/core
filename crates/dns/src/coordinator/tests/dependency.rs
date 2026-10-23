use super::*;

#[tokio::test]
async fn recursive_resolution_rejects_its_own_actual_flight_without_breaking_independent_coalescing(
) {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let nested = coordinator.clone();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        coordinator.resolve(key(7), async move {
            nested.resolve(key(7), async { Ok(vec![]) }).await
        }),
    )
    .await
    .expect("recursion must fail instead of waiting for its own flight")
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("dependency cycle"));
    coordinator
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn the_same_question_in_another_actual_resolver_is_not_a_dependency_cycle() {
    let first = QueryCoordinator::<Vec<IpAddress>>::default();
    let second = QueryCoordinator::<Vec<IpAddress>>::default();
    let inner = second.clone();
    let result = first
        .resolve(key(7), async move {
            inner
                .resolve(key(7), async { Ok(vec![IpAddress::V4([192, 0, 2, 9])]) })
                .await
        })
        .await
        .unwrap();
    assert_eq!(result, vec![IpAddress::V4([192, 0, 2, 9])]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    first.shutdown(deadline).await.unwrap();
    second.shutdown(deadline).await.unwrap();
}

#[tokio::test]
async fn actual_worker_panic_fails_waiters_and_remains_an_observable_shutdown_failure() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        coordinator.resolve(key(7), async {
            panic!("controlled actual DNS worker panic")
        }),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("controlled actual DNS worker panic"));
    assert!(coordinator
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .is_err());
    assert!(coordinator.state.lock().unwrap().tasks.is_empty());
}

#[tokio::test]
async fn shared_dns_failure_retains_the_actual_native_error_source() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let native = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(file.path())
        .unwrap_err();
    let code = native.raw_os_error().unwrap();
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let error = coordinator
        .resolve(key(7), async move { Err(native) })
        .await
        .unwrap_err();
    let mut source: &(dyn std::error::Error + 'static) = &error;
    loop {
        if source
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.raw_os_error() == Some(code))
        {
            break;
        }
        source = source
            .source()
            .expect("native source must survive coalescing");
    }
    coordinator
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn independently_started_leaders_cannot_form_a_mutual_wait_cycle() {
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let mut other = key(7);
    other.domain = "independent.test".into();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut callers = Vec::new();
    for (own, next) in [(key(7), other.clone()), (other, key(7))] {
        let coordinator = coordinator.clone();
        let nested = coordinator.clone();
        let barrier = barrier.clone();
        callers.push(tokio::spawn(async move {
            coordinator
                .resolve(own, async move {
                    barrier.wait().await;
                    nested
                        .resolve(next, async {
                            panic!("both actual leaders are already registered")
                        })
                        .await
                })
                .await
        }));
    }
    for caller in callers {
        let error = tokio::time::timeout(Duration::from_secs(1), caller)
            .await
            .expect("mutual dependency cannot hang")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
    coordinator
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(coordinator.graph.lock().unwrap().edges.is_empty());
}
