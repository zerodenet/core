use super::*;
use hyper::rt::Executor as _;
use tokio::sync::mpsc::error::TrySendError;

#[tokio::test]
async fn actual_hyper_delegate_rejection_wakes_pending_request_with_the_same_original_source() {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        // The real connection Task consumes this one slot. The next original
        // Pipe/Send cannot be admitted, even after Task moved out of the queue.
        let mut pending = spawn::<Full<Bytes>>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(1, CancellationToken::new()),
        )
        .unwrap();
        let (established, ready) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut h2 = h2::server::handshake(peer).await.unwrap();
            established.send(()).unwrap();
            while let Some(result) = h2.accept().await {
                if result.is_err() {
                    break;
                }
            }
        });
        let mut sender = pending.ready().await.unwrap();
        ready.await.unwrap();
        let mut sending = Box::pin(sender.send_request(request(b"never retried")));
        pending_once(sending.as_mut()).await;
        let source = match sending.await {
            Err(source) => source,
            Ok(_) => panic!("finite real delegate admission must reject"),
        };
        assert_eq!(source.reason, Reason::Capacity);
        assert_eq!(source.stage, Stage::Prepare);
        assert!(matches!(
            source.source().unwrap().downcast_ref::<TrySendError<()>>(),
            Some(TrySendError::Full(()))
        ));
        let later = sender.ready().await.unwrap_err();
        assert!(Arc::ptr_eq(&source, &later));
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, pending.task_id());
        let joined = pending.join_witness().wait().await.result().unwrap_err();
        let observed = joined
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<ChannelError>()
            .unwrap();
        assert!(std::ptr::eq(observed, source.as_ref()));
        assert!(adoption.closed().is_none());
        assert!(adoption.retire(end()).await.unwrap().is_complete());
        server.await.unwrap();
        assert!(scope.shutdown(end()).await.is_complete());
        assert!(fixture.owner.shutdown(end()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn active_and_queued_delegates_share_one_finite_budget_and_error_retains_no_rejected_future()
{
    let failure = Arc::new(Failure::new(instance()));
    let inbox = executor::Inbox::new(NonZeroUsize::new(1).unwrap(), failure.clone());
    let executor = inbox.executor();
    executor.execute(std::future::pending::<()>());
    let active = inbox.pop().unwrap();
    assert_eq!(inbox.occupied(), 1, "moving to active must not free budget");
    let retained = Arc::new(());
    let weak = Arc::downgrade(&retained);
    executor.execute(async move {
        let _retained = retained;
        std::future::pending::<()>().await;
    });
    let source = failure.get().unwrap();
    assert_eq!(source.reason, Reason::Capacity);
    assert!(
        weak.upgrade().is_none(),
        "first source contains (), never rejected future or IO"
    );
    assert!(matches!(
        source.source().unwrap().downcast_ref::<TrySendError<()>>(),
        Some(TrySendError::Full(()))
    ));
    drop(active);
    assert_eq!(inbox.occupied(), 0);
    inbox.close();
    executor.execute(std::future::pending::<()>());
    assert!(
        Arc::ptr_eq(&source, &failure.get().unwrap()),
        "later rejection preserves first original Arc"
    );
    drop(inbox);
    executor.execute(std::future::pending::<()>());
    assert!(Arc::ptr_eq(&source, &failure.get().unwrap()));
}
