use super::*;

#[tokio::test]
async fn real_tcp_reset_keeps_original_os_and_connmaperr_h2_objects_before_actual_join_and_close() {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let cancel = CancellationToken::new();
        let mut pending = spawn::<Full<Bytes>>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(8, cancel.clone()),
        )
        .unwrap();
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (reset, resetting) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            peer.set_zero_linger().unwrap();
            let mut h2 = h2::server::handshake(peer).await.unwrap();
            let request = h2.accept().await.unwrap().unwrap();
            entered.send(()).unwrap();
            resetting.await.unwrap();
            drop((request, h2));
        });
        let mut sender = pending.ready().await.unwrap();
        let mut sending = Box::pin(sender.send_request(request(b"")));
        pending_once(sending.as_mut()).await;
        entering.await.unwrap();
        reset.send(()).unwrap();
        let first = match sending.await {
            Err(source) => source,
            Ok(_) => panic!("real TCP reset must fail the pending request"),
        };
        assert_eq!(first.reason, Reason::Io);
        assert_eq!(first.impact, Impact::Component);
        let before = fields(&first);
        let evidence = pending.failure_evidence();
        let io = evidence
            .io_source()
            .expect("the actual native read/write returned an error");
        assert!(
            io.raw_os_error().is_some(),
            "native code is kept on the original, not its facade"
        );
        assert!(io.get_ref().is_none());
        let protocol = evidence
            .h2_source_arc()
            .expect("optional ConnMapErr hook consumed its original h2 object");
        assert!(protocol.is_io());
        assert!(
            protocol.source().is_none(),
            "pinned h2 does not expose its IO through Error::source"
        );
        assert_eq!(
            protocol.get_io().unwrap().raw_os_error(),
            None,
            "h2 has already reconstructed its own IO facade"
        );
        let published = first
            .source()
            .unwrap()
            .downcast_ref::<H2ConnectionFailure>()
            .unwrap();
        assert!(Arc::ptr_eq(io, published.io_source().unwrap()));
        assert!(Arc::ptr_eq(protocol, published.h2_source_arc().unwrap()));
        let from_chain = published
            .source()
            .unwrap()
            .downcast_ref::<h2::Error>()
            .unwrap();
        assert!(std::ptr::eq(from_chain, protocol.as_ref()));
        cancel.cancel();
        assert!(Arc::ptr_eq(&first, &sender.ready().await.unwrap_err()));
        assert_eq!(
            fields(&first),
            before,
            "late cancellation and h2 evidence do not replace any decision field"
        );
        assert_eq!(fixture.owner.retained(), 1);
        finish(&mut fixture, &mut scope, &adoption, pending.task_id()).await;
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_observed_before_peer_reset_keeps_first_arc_without_inventing_an_unpolled_io_error(
) {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let cancel = CancellationToken::new();
        let mut pending = spawn::<Full<Bytes>>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(8, cancel.clone()),
        )
        .unwrap();
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (reset, resetting) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            peer.set_zero_linger().unwrap();
            let mut h2 = h2::server::handshake(peer).await.unwrap();
            let request = h2.accept().await.unwrap().unwrap();
            entered.send(()).unwrap();
            resetting.await.unwrap();
            drop((request, h2));
        });
        let mut sender = pending.ready().await.unwrap();
        let mut sending = Box::pin(sender.send_request(request(b"")));
        pending_once(sending.as_mut()).await;
        entering.await.unwrap();
        cancel.cancel();
        let first = match sending.await {
            Err(source) => source,
            Ok(_) => panic!("explicit lifetime cancellation must wake pending request"),
        };
        assert_eq!(first.reason, Reason::Cancelled);
        let before = fields(&first);
        reset.send(()).unwrap();
        server.await.unwrap();
        assert!(Arc::ptr_eq(&first, &sender.ready().await.unwrap_err()));
        assert_eq!(fields(&first), before);
        assert!(
            pending.failure_evidence().io_source().is_none(),
            "no IO future was polled after aggregate cancellation"
        );
        assert!(pending.failure_evidence().h2_source().is_none());
        finish(&mut fixture, &mut scope, &adoption, pending.task_id()).await;
    })
    .await
    .unwrap();
}
