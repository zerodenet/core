use super::*;
#[tokio::test]
async fn same_actual_pool_has_one_complete_cold_recipe_and_admission_checks_full_values() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut fixture = Fixture::cold(listener.local_addr().unwrap(), 1, 2, None, None);
    let options = fixture.options.as_ref();
    let same = NativePoolOptions::from_pool(
        &fixture.pool,
        options.settings.clone(),
        options.carrier_limit,
        options.stream_limit,
    )
    .unwrap();
    assert_eq!(same.identity, options.identity);
    let mut changed = options.settings.clone();
    changed.grpc.initial_window_size = 262_144;
    assert!(NativePoolOptions::from_pool(
        &fixture.pool,
        changed.clone(),
        options.carrier_limit,
        options.stream_limit
    )
    .is_err());
    assert!(NativePoolOptions::from_pool(
        &fixture.pool,
        options.settings.clone(),
        NonZeroUsize::new(2).unwrap(),
        options.stream_limit
    )
    .is_err());
    assert!(NativePoolOptions::from_pool(
        &fixture.pool,
        options.settings.clone(),
        options.carrier_limit,
        NonZeroUsize::new(3).unwrap()
    )
    .is_err());
    let other_pool = GrpcPool::default();
    let other = NativePoolOptions::from_pool(
        &other_pool,
        options.settings.clone(),
        options.carrier_limit,
        options.stream_limit,
    )
    .unwrap();
    assert_ne!(other.identity, options.identity);
    assert!(!fixture.pool.shares_state(&other_pool));
    fixture.start().await;
    assert!(same.matches(fixture.actual).is_ok());
    let mut forged = same.clone();
    forged.settings = changed;
    assert_eq!(
        forged.matches(fixture.actual).unwrap_err().reason,
        Reason::StalePlan
    );
    let mut forged = same.clone();
    forged.carrier_limit = NonZeroUsize::new(2).unwrap();
    assert_eq!(
        forged.matches(fixture.actual).unwrap_err().reason,
        Reason::StalePlan
    );
    let mut forged = same.clone();
    forged.stream_limit = NonZeroUsize::new(3).unwrap();
    assert_eq!(
        forged.matches(fixture.actual).unwrap_err().reason,
        Reason::StalePlan
    );
    // A copied hint cannot find the managed owner in a different actual State.
    let mut forged = other;
    forged.identity = same.identity;
    assert_eq!(
        forged.matches(fixture.actual).unwrap_err().reason,
        Reason::Revoked
    );
    let (protocol, tasks) = fixture.close().await;
    assert!(protocol.is_complete());
    assert!(protocol.closed_io.is_empty());
    assert!(tasks.exited_tasks.is_empty());
}
