use super::*;
#[tokio::test]
async fn registered_driver_reuses_same_pool_and_clear_cannot_recycle_even_an_actually_joined_slot()
{
    let pool = Arc::new(ClientPool::default());
    let opener = Arc::new(Loopback::default());
    let (mut scope, ctx) = context_with(1, opener.clone());
    let actual = ctx.instance();
    let mut owner = PoolFactory
        .build(
            Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),
            ctx,
        )
        .unwrap();
    owner.lifecycle.start(deadline()).await.unwrap();
    let state = pool.managed(actual).unwrap();
    let (address, server) = echo().await;
    let mut first = pool
        .open_managed_tcp(
            actual,
            key_at("one", address),
            &Default::default(),
            Some(deadline()),
        )
        .await
        .unwrap();
    let mut second = pool
        .open_managed_tcp(
            actual,
            key_at("one", address),
            &Default::default(),
            Some(deadline()),
        )
        .await
        .unwrap();
    first.write_all(b"authentic encrypted wire").await.unwrap();
    let mut received = [0; 24];
    first.read_exact(&mut received).await.unwrap();
    assert_eq!(&received, b"authentic encrypted wire");
    assert_eq!(pool.snapshot().connections, 1);
    assert_eq!(opener.opens.load(Ordering::SeqCst), 1);
    pool.clear();
    drop(first);
    second.write_all(b"live").await.unwrap();
    let mut received = [0; 4];
    second.read_exact(&mut received).await.unwrap();
    assert_eq!(&received, b"live");
    drop(second);
    let denied = pool
        .open_managed_tcp(
            actual,
            key_at("two", address),
            &Default::default(),
            Some(deadline()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(denied.kind(), io::ErrorKind::WouldBlock);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), state.drained())
            .await
            .is_err(),
        "a driver body ending must not manufacture its actual scope join"
    );
    let joined = tokio::time::timeout(Duration::from_secs(2), scope.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert!(joined.result.is_ok());
    tokio::time::timeout(Duration::from_secs(2), state.drained())
        .await
        .unwrap();
    assert_eq!(
        joined.class,
        zero_platform_tokio::contract::lifecycle::TaskClass::Critical
    );
    let denied = pool
        .open_managed_tcp(
            actual,
            key_at("three", address),
            &Default::default(),
            Some(deadline()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(denied.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(opener.opens.load(Ordering::SeqCst), 1);
    server.await.unwrap();
    let end = deadline();
    let (protocol, tasks) = tokio::join!(owner.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(matches!(
        protocol.unwrap().unfinished.as_slice(),
        [Unfinished::Io(_)]
    ));
    assert!(tasks.is_complete());
}

#[tokio::test]
async fn actual_registration_capacity_failure_never_starts_an_unowned_replacement_driver() {
    let pool = Arc::new(ClientPool::default());
    let opener = Arc::new(Loopback::default());
    let (mut scope, ctx) = context_with(0, opener.clone());
    let actual = ctx.instance();
    let mut owner = PoolFactory
        .build(
            Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),
            ctx,
        )
        .unwrap();
    owner.lifecycle.start(deadline()).await.unwrap();
    let (address, server) = echo().await;
    let error = pool
        .open_managed_tcp(
            actual,
            key_at("capacity", address),
            &Default::default(),
            Some(deadline()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<zero_platform_tokio::contract::scope::SpawnError>(),
        Some(&zero_platform_tokio::contract::scope::SpawnError::Capacity)
    );
    let error = pool
        .open_managed_tcp(
            actual,
            key_at("another", address),
            &Default::default(),
            Some(deadline()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(opener.opens.load(Ordering::SeqCst), 1);
    server.await.unwrap();
    let end = deadline();
    let (protocol, tasks) = tokio::join!(owner.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(matches!(
        protocol.unwrap().unfinished.as_slice(),
        [Unfinished::Io(_)]
    ));
    assert!(tasks.is_complete() && tasks.exited_tasks.is_empty());
}
#[tokio::test]
async fn registered_driver_failure_retains_original_io_payload_and_actual_scope_join_result() {
    let pool = Arc::new(ClientPool::default());
    let control = Arc::new(FailureControl::default());
    let marker = Arc::new(());
    let opener = Arc::new(Loopback {
        failure: Some((control.clone(), marker.clone())),
        ..Default::default()
    });
    let (mut scope, ctx) = context_with(1, opener);
    let actual = ctx.instance();
    let mut owner = PoolFactory
        .build(
            Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap())),
            ctx,
        )
        .unwrap();
    owner.lifecycle.start(deadline()).await.unwrap();
    let (address, server) = echo().await;
    let mut stream = pool
        .open_managed_tcp(
            actual,
            key_at("source", address),
            &Default::default(),
            Some(deadline()),
        )
        .await
        .unwrap();
    stream.write_all(b"real").await.unwrap();
    let mut payload = [0; 4];
    stream.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"real");
    control.trigger();
    let exit = tokio::time::timeout(Duration::from_secs(2), scope.next_exit())
        .await
        .unwrap()
        .unwrap();
    let error = exit.result.unwrap_err();
    assert_eq!(error.stage, Stage::Receive);
    assert_eq!(error.reason, Reason::Io);
    assert_eq!(error.impact, Impact::Component);
    let original =
        original_failure(&error).expect("same rich I/O payload through tracked scope forwarding");
    assert!(Arc::ptr_eq(&original.0, &marker));
    assert!(stream.read(&mut payload).await.is_err());
    drop(stream);
    pool.clear();
    server.await.unwrap();
    let end = deadline();
    let (protocol, tasks) = tokio::join!(owner.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(matches!(
        protocol.unwrap().unfinished.as_slice(),
        [Unfinished::Io(_)]
    ));
    assert!(tasks.is_complete());
}

fn original_failure<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a OriginalFailure> {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(io) = source.downcast_ref::<io::Error>() {
            assert_eq!(io.kind(), io::ErrorKind::ConnectionReset);
            if let Some(original) = io
                .get_ref()
                .and_then(|source| source.downcast_ref::<OriginalFailure>())
            {
                return Some(original);
            }
        }
        current = source.source();
    }
    None
}
