use super::*;
#[tokio::test]
async fn unix_target_uses_the_same_original_graph_native_view_actor_and_host_ack() {
    timeout(Duration::from_secs(3), async {
        let files = tempfile::tempdir().unwrap();
        let path = files.path().join("actual-target.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let path = path.to_str().unwrap().to_owned();
        let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope = hosted(1);
        let endpoint = FallbackEndpoint::Unix { path: path.clone() };
        let pool = pool(&scope, &root, 1, endpoint.clone());
        let original = pool
            .reserve_graph(&scope.registrar().scope_identity(), &endpoint)
            .unwrap();
        original.begin_registration().unwrap();
        let actual = original.clone();
        let (begin, started) = tokio::sync::oneshot::channel();
        let (ready, received) = tokio::sync::oneshot::channel();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                started.await.unwrap();
                assert!(actual
                    .prepare_unix(&(path.clone() + ".other"))
                    .await
                    .is_err());
                let peer = "127.0.0.1:9".parse().unwrap();
                let egress = crate::EgressInterfaceControl::default();
                let policy = DialPolicy::default();
                let selection = egress.select_for_peer_with_policy(peer, &policy).unwrap();
                assert!(actual
                    .prepare_tcp(peer, &policy, &selection, None)
                    .await
                    .is_err());
                assert!(!actual.0.acquired.load(Ordering::Acquire));
                let (mut pending, _) = actual.prepare_unix(&path).await.unwrap();
                let stream = pending.connect(end()).await.unwrap();
                drop(pending);
                let view = actual
                    .wrap_stream(TcpRelayStream::from_client(stream))
                    .unwrap();
                assert!(ready.send(view).is_ok());
                actual.wait_view_returned().await;
                Ok(())
            })
            .unwrap();
        original.bind_driver(witness.clone()).unwrap();
        begin.send(()).unwrap();
        let (view, accepted) = tokio::join!(received, listener.accept());
        let mut view = view.unwrap();
        let mut peer = accepted.unwrap().0;
        view.write_all(b"unix original").await.unwrap();
        let mut bytes = [0; 13];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"unix original");
        drop(view);
        let receipt = scope.next_receipt().await.unwrap();
        receipt.result().unwrap();
        assert!(witness.matches_joined(receipt.joined()));
        assert!(original.close(end()).await.unwrap().is_complete());
        let closed = original.closed().unwrap();
        assert!(closed.matches(&original) && closed.had_native_acquisition());
        assert_eq!(closed.native_originals_closed(), 1);
        assert!(!closed.registration_released());
        assert_eq!(root.snapshot().charged, 1);
        scope.acknowledge(&receipt).unwrap();
        pump(&root).await;
        assert!(closed.registration_released());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
