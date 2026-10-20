use super::*;

#[tokio::test]
async fn original_h2_wire_runs_in_one_registered_aggregate_and_native_close_waits_for_join() {
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
        let witness = pending.join_witness().clone();
        let id = pending.task_id();
        assert!(!witness.is_joined());
        assert_eq!(
            peer.try_read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "registration does not poll the inline handshake"
        );
        {
            let mut waiting = Box::pin(pending.ready());
            pending_once(waiting.as_mut()).await;
        }
        assert_eq!(
            fixture.owner.retained(),
            1,
            "cancelling ready wait refunds no physical capacity"
        );
        let server = tokio::spawn(async move {
            let mut h2 = h2::server::handshake(peer).await.unwrap();
            let (mut request, mut respond) = h2.accept().await.unwrap().unwrap();
            assert_eq!(request.method(), http::Method::POST);
            assert_eq!(request.uri().path(), "/original");
            let transfer = async {
                let mut uploaded = Vec::new();
                while let Some(bytes) = request.body_mut().data().await {
                    uploaded.extend_from_slice(&bytes.unwrap());
                }
                assert_eq!(uploaded, b"one original request");
                let mut send = respond
                    .send_response(http::Response::new(()), false)
                    .unwrap();
                send.send_data(Bytes::from_static(b"actual h2 response"), true)
                    .unwrap();
            };
            let drive = async {
                while let Some(result) = h2.accept().await {
                    if result.is_err() {
                        break;
                    }
                    panic!("fixture expected exactly one request");
                }
            };
            tokio::join!(transfer, drive);
        });
        let mut sender = pending.ready().await.unwrap();
        let response = sender
            .send_request(request(b"one original request"))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"actual h2 response"[..]
        );
        cancel.cancel();
        // This exact wait does not drive JoinSet, even after aggregate body exit.
        let mut joining = Box::pin(witness.wait());
        pending_once(joining.as_mut()).await;
        drop(joining);
        assert!(adoption.closed().is_none());
        assert_eq!(
            fixture.owner.grants().reserve().err().unwrap().reason,
            Reason::Capacity
        );
        let incomplete = adoption.retire(Instant::now()).await.unwrap();
        assert!(!incomplete.is_complete());
        assert!(adoption.closed().is_none());
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        assert!(witness.is_joined());
        assert!(
            adoption.closed().is_none(),
            "actual aggregate join is not native closure"
        );
        let original = witness.wait().await.result().unwrap_err();
        let again = sender.ready().await.unwrap_err();
        // Scope's forwarding layer reaches the same original connection error.
        let observed = original
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<ChannelError>()
            .unwrap();
        assert!(std::ptr::eq(observed, again.as_ref()));
        let closed = adoption.retire(end()).await.unwrap();
        assert!(closed.is_complete());
        assert_eq!(closed.closed_io.len(), 1);
        let receipt = adoption.closed().unwrap();
        assert!(receipt.matches(&adoption));
        assert_eq!(receipt.driver_exit().unwrap().id(), id);
        assert_eq!(fixture.owner.retained(), 0);
        server.await.unwrap();
        assert!(scope.shutdown(end()).await.is_complete());
        assert!(fixture.owner.shutdown(end()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_unclaimed_handshake_result_never_refunds_original_native_capacity() {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, mut peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let pending = spawn::<Full<Bytes>>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(4, CancellationToken::new()),
        )
        .unwrap();
        let id = pending.task_id();
        let witness = pending.join_witness().clone();
        drop(pending);
        // The actual registered handshake writes the real client preface; its
        // dropped consumer then terminates this same aggregate, not a new task.
        let mut preface = [0; 24];
        peer.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        assert_eq!(fixture.owner.retained(), 1);
        assert!(adoption.closed().is_none());
        assert_eq!(
            fixture.owner.grants().reserve().err().unwrap().reason,
            Reason::Capacity
        );
        assert_eq!(scope.next_exit().await.unwrap().id, id);
        assert!(witness.is_joined());
        assert!(adoption.closed().is_none());
        assert!(adoption.retire(end()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(fixture.owner.retained(), 0);
        assert!(scope.shutdown(end()).await.is_complete());
        assert!(fixture.owner.shutdown(end()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}
