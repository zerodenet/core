//! Real target originals, unique IO, one actor and separately held Host ACK.
use super::*;
use crate::contract::scope::{HostedScope, TaskClass, TaskScope};
use std::{future::poll_fn, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use zero_core::component::{ComponentId, ResourceGeneration};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(991),
        generation: ResourceGeneration(9),
    }
}
fn hosted(n: usize) -> HostedScope {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(n));
    scope.enable_registration().unwrap();
    scope
}
fn pool(
    scope: &HostedScope,
    root: &Arc<CarrierOriginalTotals>,
    n: usize,
    endpoint: FallbackEndpoint,
) -> InboundTargetSocketPool {
    InboundTargetSocketPool::new(
        instance(),
        scope.registrar().retirement_observation(),
        root,
        NonZeroUsize::new(n).unwrap(),
        Arc::from([endpoint]),
        CancellationToken::new(),
    )
}
async fn pump(root: &Arc<CarrierOriginalTotals>) {
    poll_fn(|cx| {
        root.poll_cancelled_retirements(cx);
        if root.snapshot().charged == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}
async fn wire(stream: &mut TcpRelayStream, peer: &mut tokio::net::TcpStream) {
    stream.write_all(b"same target").await.unwrap();
    stream.flush().await.unwrap();
    let mut bytes = [0; 11];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"same target");
    peer.write_all(&bytes).await.unwrap();
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"same target");
}
#[tokio::test]
async fn target_graph_retries_same_n1_only_after_actual_failed_native_close_and_holds_all_credit_until_actor_ack(
) {
    timeout(Duration::from_secs(3), async {
        let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope = hosted(1);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let endpoint = FallbackEndpoint::Tcp {
            server: "actual-target.test".into(),
            port: peer.port(),
        };
        let pool = pool(&scope, &root, 1, endpoint.clone());
        let original = pool
            .reserve_graph(&scope.registrar().scope_identity(), &endpoint)
            .unwrap();
        assert_eq!(root.snapshot().charged, 1);
        original.begin_registration().unwrap();
        let (begin, started) = tokio::sync::oneshot::channel();
        let (ready, delivered) = tokio::sync::oneshot::channel();
        let actual = original.clone();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                started.await.unwrap();
                let egress = crate::EgressInterfaceControl::default();
                let policy = DialPolicy::default();
                let wrong = SocketAddr::new(
                    peer.ip(),
                    if peer.port() == u16::MAX {
                        peer.port() - 1
                    } else {
                        peer.port() + 1
                    },
                );
                let selection = egress.select_for_peer_with_policy(wrong, &policy).unwrap();
                assert!(actual
                    .prepare_tcp(wrong, &policy, &selection, None)
                    .await
                    .is_err());
                #[cfg(unix)]
                assert!(actual
                    .prepare_unix("/not-the-declared-tcp-target")
                    .await
                    .is_err());
                assert!(!actual.0.acquired.load(Ordering::Acquire));
                assert!(actual.0.state.lock().await.slots.is_empty());
                let bad = SocketAddr::new("127.0.0.2".parse().unwrap(), peer.port());
                let selection = egress.select_for_peer_with_policy(bad, &policy).unwrap();
                let (mut pending, index) = actual
                    .prepare_tcp(bad, &policy, &selection, None)
                    .await
                    .unwrap();
                assert!(pending.connect(end()).await.is_err());
                drop(pending);
                assert!(actual.close_candidate(index, end()).await.unwrap());
                let selection = egress.select_for_peer_with_policy(peer, &policy).unwrap();
                let (mut pending, _) = actual
                    .prepare_tcp(peer, &policy, &selection, None)
                    .await
                    .unwrap();
                let stream = pending.connect(end()).await.unwrap();
                drop(pending);
                let stream = actual
                    .wrap_stream(TcpRelayStream::from_client(stream))
                    .unwrap();
                assert!(ready.send(stream).is_ok());
                actual.wait_view_returned().await;
                Ok(())
            })
            .unwrap();
        original.bind_driver(witness.clone()).unwrap();
        begin.send(()).unwrap();
        let (view, peer) = tokio::join!(delivered, listener.accept());
        let mut view = view.unwrap();
        let mut peer = peer.unwrap().0;
        wire(&mut view, &mut peer).await;
        {
            let mut closing = Box::pin(original.close(end()));
            poll_fn(|cx| {
                assert!(closing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert!(original.closed().is_none() && !witness.is_joined());
        assert_eq!(root.snapshot().charged, 1);
        drop(view);
        let receipt = scope.next_receipt().await.unwrap();
        assert!(witness.matches_joined(receipt.joined()));
        receipt.result().unwrap();
        let report = original.close(end()).await.unwrap();
        assert!(report.is_complete());
        let closed = original.closed().unwrap();
        assert!(closed.matches(&original) && closed.had_native_acquisition());
        assert_eq!(closed.native_originals_closed(), 2);
        assert!(!closed.registration_released());
        assert_eq!(root.snapshot().charged, 1);
        assert_eq!(pool.0.permits.available_permits(), 0);
        assert!(pool
            .reserve_graph(&scope.registrar().scope_identity(), &endpoint)
            .is_err());
        scope.acknowledge(&receipt).unwrap();
        pump(&root).await;
        assert!(closed.registration_released());
        assert_eq!(pool.0.permits.available_permits(), 1);
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert!(scope.registrar().scope_identity().registration_open());
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
mod cancellation;
mod parallel;
#[cfg(unix)]
mod unix;
