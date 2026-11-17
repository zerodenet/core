use super::*;
#[derive(Debug)]
struct PanicBoundary;
impl IoObserver for PanicBoundary {
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
    fn stream_boundary(&self) {
        panic!("actual target callback after native custody installation");
    }
}
#[tokio::test]
async fn target_callback_panic_retains_actual_pre_registration_native_until_original_join_and_ack()
{
    timeout(Duration::from_secs(3), async {
        let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope = hosted(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let endpoint = FallbackEndpoint::Tcp {
            server: peer.ip().to_string(),
            port: peer.port(),
        };
        let pool = pool(&scope, &root, 1, endpoint.clone());
        let original = pool
            .reserve_graph(&scope.registrar().scope_identity(), &endpoint)
            .unwrap();
        original.begin_registration().unwrap();
        let (begin, started) = tokio::sync::oneshot::channel();
        let actual = original.clone();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                started.await.unwrap();
                let egress = crate::EgressInterfaceControl::default();
                let policy = DialPolicy::default();
                let selection = egress.select_for_peer_with_policy(peer, &policy).unwrap();
                let _ = actual
                    .prepare_tcp(peer, &policy, &selection, Some(Arc::new(PanicBoundary)))
                    .await;
                unreachable!("the actual boundary panics after storing the same native controller");
            })
            .unwrap();
        original.bind_driver(witness.clone()).unwrap();
        begin.send(()).unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        let fault = receipt.result().unwrap_err();
        assert!(witness.matches_joined(receipt.joined()));
        assert!(Arc::ptr_eq(
            &fault,
            &witness.try_joined().unwrap().result().unwrap_err()
        ));
        assert_eq!(root.snapshot().charged, 1);
        assert!(original.closed().is_none());
        let report = original.close(end()).await.unwrap();
        assert!(report.is_complete());
        let closed = original.closed().unwrap();
        assert!(closed.matches(&original) && closed.had_native_acquisition());
        assert_eq!(closed.native_originals_closed(), 1);
        assert!(!closed.registration_released());
        assert_eq!(root.snapshot().charged, 1);
        // The callback ran before connect/AsyncFd registration. Absence of an
        // accepted peer is not our native receipt: the same controller above is.
        poll_fn(|cx| {
            assert!(listener.poll_accept(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(original);
        scope.acknowledge(&receipt).unwrap();
        pump(&root).await;
        assert!(closed.registration_released());
        assert_eq!(pool.0.permits.available_permits(), 1);
        assert!(Arc::ptr_eq(&fault, &receipt.result().unwrap_err()));
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn cancellation_before_connect_keeps_the_same_native_and_lost_owner_until_real_actor_join() {
    timeout(Duration::from_secs(3), async {
        let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope = hosted(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let endpoint = FallbackEndpoint::Tcp {
            server: peer.ip().to_string(),
            port: peer.port(),
        };
        let pool = pool(&scope, &root, 1, endpoint.clone());
        let original = pool
            .reserve_graph(&scope.registrar().scope_identity(), &endpoint)
            .unwrap();
        original.begin_registration().unwrap();
        let (begin, started) = tokio::sync::oneshot::channel();
        let (ready, reached) = tokio::sync::oneshot::channel();
        let actual = original.clone();
        let stop = original.cancellation();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                started.await.unwrap();
                let egress = crate::EgressInterfaceControl::default();
                let policy = DialPolicy::default();
                let selection = egress.select_for_peer_with_policy(peer, &policy).unwrap();
                let (pending, _) = actual
                    .prepare_tcp(peer, &policy, &selection, None)
                    .await
                    .unwrap();
                ready.send(()).unwrap();
                actual.cancellation().cancelled().await;
                drop(pending);
                Ok(())
            })
            .unwrap();
        original.bind_driver(witness.clone()).unwrap();
        begin.send(()).unwrap();
        reached.await.unwrap();
        let core = original.0.clone();
        assert!(core.acquired.load(Ordering::Acquire));
        assert!(!core.closed.load(Ordering::Acquire));
        assert_eq!(root.snapshot().charged, 1);
        assert!(!witness.is_joined());
        drop(original);
        stop.cancel();
        let collect = poll_fn(|cx| {
            root.poll_cancelled_retirements(cx);
            if core.closed.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        let (receipt, ()) = tokio::join!(scope.next_receipt(), collect);
        let receipt = receipt.unwrap();
        receipt.result().unwrap();
        assert!(witness.matches_joined(receipt.joined()));
        assert_eq!(root.snapshot().charged, 1);
        assert_eq!(pool.0.permits.available_permits(), 0);
        scope.acknowledge(&receipt).unwrap();
        pump(&root).await;
        assert_eq!(pool.0.permits.available_permits(), 1);
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
