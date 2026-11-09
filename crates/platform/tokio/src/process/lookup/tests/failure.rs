//! A real OS read failure is task custody, not a second native close fault.
use super::*;

#[tokio::test]
async fn ordinary_failed_lookup_retains_original_until_exact_receiver_and_reuses_one_root_unit() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (_server, _) = listener.accept().unwrap();
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(2).unwrap());
    let registrar = scope.registrar();
    let root = crate::controlled_carrier::CarrierOriginalTotals::new(NonZeroUsize::MIN);
    let (mut reader, mut writer) = tokio::io::duplex(1);
    let (pending, observed) = tokio::sync::oneshot::channel();
    let (caller_id, caller) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            let read = reader.read_u8();
            tokio::pin!(read);
            let mut pending = Some(pending);
            poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(pending) = pending.take() {
                        pending.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .map_err(|source| error(Stage::Receive, Reason::Io).with_source(source))?;
            Ok(())
        })
        .unwrap();
    observed.await.unwrap();
    let prepare = |limit| {
        let original = LocalProcessLookup::prepare(
            caller.clone(),
            tcp.local_addr().unwrap(),
            LocalProcessTransport::Tcp,
            registrar.cancellation(),
            limit,
        )
        .unwrap();
        original
            .bind_original_totals(instance(), &registrar.downgrade(), &root)
            .unwrap();
        original
    };
    let original = prepare(NonZeroUsize::MIN);
    original.register(&registrar.downgrade()).unwrap().start();
    assert_eq!(
        original
            .result(&CancellationToken::new())
            .await
            .unwrap_err()
            .reason,
        Reason::Capacity
    );
    let worker = original.witness().unwrap();
    let event = scope.next_exit().await.unwrap();
    assert_eq!(event.id, worker.task_id().unwrap());
    let joined = worker.wait().await;
    let source = joined.result().unwrap_err();
    assert!(joined.registration_released());
    assert!(!joined.failure_accepted());
    original.collect_native_after_join().unwrap();
    assert!(original.native.confirmed());
    assert!(!original.retired());
    assert_eq!(root.snapshot().charged, 1);
    assert!(
        root.failures().is_empty(),
        "original task error cannot create a different ResourceFailure"
    );
    let held = original
        .wait_retired(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap_err();
    let retained = std::error::Error::source(&held)
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap();
    assert!(Arc::ptr_eq(retained, &source));
    let denied = prepare(NonZeroUsize::new(8 * 1024 * 1024).unwrap());
    let error = denied.register(&registrar.downgrade()).err().unwrap();
    assert_eq!(error.reason, Reason::Capacity);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    assert!(denied.witness().is_none());
    // The actual unique owner strongly stores its delivered TaskExit source.
    let received = event.result.unwrap_err();
    worker.acknowledge_hosted_failure(&joined).unwrap();
    assert!(
        !joined.failure_accepted(),
        "ordinary hosted API remains a no-op"
    );
    scope
        .accept_retained_failure(&worker, &joined, &received)
        .unwrap();
    assert!(joined.failure_accepted());
    // A busy native observation is still Unknown even after task reception.
    let native = original.native.originals();
    assert!(!original.retired());
    assert_eq!(root.snapshot().charged, 1);
    drop(native);
    original
        .wait_retired(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(original.retired());
    assert_eq!(root.snapshot().charged, 0);
    assert_eq!(source.reason, Reason::Capacity); // original failure remains retained
    let next = prepare(NonZeroUsize::new(8 * 1024 * 1024).unwrap());
    next.register(&registrar.downgrade()).unwrap().start();
    assert_eq!(
        next.result(&CancellationToken::new())
            .await
            .unwrap()
            .unwrap()
            .pid,
        std::process::id()
    );
    let event = scope.next_exit().await.unwrap();
    assert_eq!(event.id, next.witness().unwrap().task_id().unwrap());
    event.result.unwrap();
    next.wait_retired(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(root.snapshot().charged, 0);
    assert!(!caller.is_joined());
    writer.write_u8(1).await.unwrap();
    let event = scope.next_exit().await.unwrap();
    assert_eq!(event.id, caller_id);
    event.result.unwrap();
    assert!(scope.is_drained());
}
