use super::*;
#[tokio::test]
async fn one_graph_keeps_real_parallel_candidates_inside_the_same_local_socket_limit() {
    timeout(Duration::from_secs(3), async {
        let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope = hosted(1);
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = first.local_addr().unwrap();
        let second = TcpListener::bind(SocketAddr::new("127.0.0.2".parse().unwrap(), a.port()))
            .await
            .unwrap();
        let b = second.local_addr().unwrap();
        assert_eq!(a.port(), b.port());
        assert_ne!(a.ip(), b.ip());
        let endpoint = FallbackEndpoint::Tcp {
            server: "same-finite-candidate-set.test".into(),
            port: a.port(),
        };
        let pool = pool(&scope, &root, 2, endpoint.clone());
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
                let egress = crate::EgressInterfaceControl::default();
                let policy = DialPolicy::default();
                let sa = egress.select_for_peer_with_policy(a, &policy).unwrap();
                let sb = egress.select_for_peer_with_policy(b, &policy).unwrap();
                let (mut pa, ia) = actual.prepare_tcp(a, &policy, &sa, None).await.unwrap();
                let (mut pb, ib) = actual.prepare_tcp(b, &policy, &sb, None).await.unwrap();
                assert_eq!(actual.0.pool.permits.available_permits(), 0);
                assert!(
                    actual.prepare_tcp(a, &policy, &sa, None).await.is_err(),
                    "a third native cannot exceed this same Source's actual local N2"
                );
                let (oa, ob) = tokio::join!(pa.connect(end()), pb.connect(end()));
                drop(pa);
                drop(pb);
                let oa = oa.unwrap();
                let ob = ob.unwrap();
                drop(ob);
                assert!(actual.close_candidate(ib, end()).await.unwrap());
                let stream = actual.wrap_stream(TcpRelayStream::from_client(oa)).unwrap();
                assert!(ready.send((stream, ia)).is_ok());
                actual.wait_view_returned().await;
                Ok(())
            })
            .unwrap();
        original.bind_driver(witness.clone()).unwrap();
        begin.send(()).unwrap();
        let (result, a, b) = tokio::join!(received, first.accept(), second.accept());
        let (mut stream, _) = result.unwrap();
        let mut peer = a.unwrap().0;
        let mut discarded = b.unwrap().0;
        assert_eq!(
            root.snapshot().charged,
            1,
            "one composite custody record, two actual native originals"
        );
        wire(&mut stream, &mut peer).await;
        assert_eq!(discarded.read(&mut [0; 1]).await.unwrap(), 0);
        drop(stream);
        let receipt = scope.next_receipt().await.unwrap();
        receipt.result().unwrap();
        assert!(witness.matches_joined(receipt.joined()));
        assert!(original.close(end()).await.unwrap().is_complete());
        let closed = original.closed().unwrap();
        assert_eq!(closed.native_originals_closed(), 2);
        assert!(closed.had_native_acquisition() && !closed.registration_released());
        assert_eq!(pool.0.permits.available_permits(), 0);
        assert_eq!(root.snapshot().charged, 1);
        scope.acknowledge(&receipt).unwrap();
        pump(&root).await;
        assert_eq!(pool.0.permits.available_permits(), 2);
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
