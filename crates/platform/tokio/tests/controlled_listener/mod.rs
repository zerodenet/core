use super::*;
use crate::contract::{lifecycle::ChannelControl, CloseReason};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{timeout, Instant},
};
mod adoption;
#[cfg(unix)]
#[path = "failure_policy.rs"]
mod failure_policy;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
async fn listener(
    parent: CancellationToken,
) -> (
    ControlledTcpListener,
    ControlledTcpListenerControl,
    SocketAddr,
) {
    let actual = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = actual.local_addr().unwrap();
    let (view, control) = ControlledTcpListener::new(actual, parent);
    (view, control, address)
}
fn assert_bound(address: SocketAddr) {
    assert_eq!(
        std::net::TcpListener::bind(address).unwrap_err().kind(),
        io::ErrorKind::AddrInUse
    );
}

#[tokio::test]
async fn pending_accept_cancellation_wakes_without_closing_the_actual_listener_or_parent() {
    timeout(Duration::from_secs(3), async {
        let parent = CancellationToken::new();
        let (mut view, mut control, address) = listener(parent.clone()).await;
        let entered = tokio::sync::Notify::new();
        let ((), result) = tokio::join!(
            async {
                entered.notified().await;
                control.cancel(CloseReason::Shutdown);
            },
            async {
                let mut accept = Box::pin(view.accept());
                poll_fn(|cx| {
                    let result = accept.as_mut().poll(cx);
                    if result.is_pending() {
                        entered.notify_one();
                    }
                    result
                })
                .await
            }
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionAborted);
        assert!(!parent.is_cancelled());
        assert_bound(address);
        let report = control.wait_closed(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        let rebound = std::net::TcpListener::bind(address).unwrap();
        assert_eq!(rebound.local_addr().unwrap(), address);
        assert_eq!(
            view.accept().await.unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert!(control
            .wait_closed(deadline())
            .await
            .unwrap()
            .closed_io
            .is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn abandoned_accept_and_expired_close_retain_the_same_listener_until_explicit_confirmation() {
    timeout(Duration::from_secs(3), async {
        let (mut view, mut control, address) = listener(CancellationToken::new()).await;
        {
            let mut accept = Box::pin(view.accept());
            poll_fn(|cx| {
                assert!(accept.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        // A canceled accept operation does not retire the listener. The next
        // operation still accepts from the same OS listening endpoint.
        let (peer, accepted) = tokio::join!(TcpStream::connect(address), view.accept());
        let mut peer = peer.unwrap();
        let (mut accepted, remote) = accepted.unwrap();
        assert_eq!(remote, peer.local_addr().unwrap());
        peer.write_all(b"one").await.unwrap();
        let mut bytes = [0; 3];
        accepted.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"one");
        // Release these test-owned child handles without claiming a controlled
        // child close receipt. Listener confirmation never covers these handles.
        drop(peer);
        drop(accepted);
        drop(view);
        drop(control.wait_closed(deadline())); // unpolled, no close side effect
        let report = control.wait_closed(Instant::now()).await.unwrap();
        assert!(!report.is_complete());
        assert!(report.closed_io.is_empty());
        assert_bound(address);
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn listener_close_leaves_accepted_children_and_sibling_listener_independently_live() {
    timeout(Duration::from_secs(3), async {
        let parent = CancellationToken::new();
        let (mut view, mut control, address) = listener(parent.clone()).await;
        let (mut sibling, mut sibling_control, sibling_address) = listener(parent.clone()).await;
        let (peer, accepted) = tokio::join!(TcpStream::connect(address), view.accept());
        let mut peer = peer.unwrap();
        let (mut accepted, _) = accepted.unwrap();
        let report = control.wait_closed(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(
            report.closed_io.len(),
            1,
            "only the listener was explicitly closed"
        );
        assert!(!parent.is_cancelled());
        peer.write_all(b"request").await.unwrap();
        let mut request = [0; 7];
        accepted.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        accepted.write_all(b"reply").await.unwrap();
        let mut reply = [0; 5];
        peer.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
        let (sibling_peer, sibling_accepted) =
            tokio::join!(TcpStream::connect(sibling_address), sibling.accept());
        let mut sibling_peer = sibling_peer.unwrap();
        let (mut sibling_accepted, _) = sibling_accepted.unwrap();
        sibling_peer.write_all(b"x").await.unwrap();
        assert_eq!(sibling_accepted.read_u8().await.unwrap(), b'x');
        assert!(sibling_control
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete());
        // Both child channels still require their own I/O close owner. These
        // test-owned sockets are dropped here, never counted as close receipts.
        drop((peer, accepted, sibling_peer, sibling_accepted));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn parent_cancellation_wakes_pending_accept_and_expired_wait_preserves_the_bound_port() {
    timeout(Duration::from_secs(3), async {
        let parent = CancellationToken::new();
        let (mut view, mut control, address) = listener(parent.clone()).await;
        let entered = tokio::sync::Notify::new();
        let (_, result) = tokio::join!(
            async {
                entered.notified().await;
                parent.cancel();
            },
            async {
                let mut accept = Box::pin(view.accept());
                poll_fn(|cx| {
                    let result = accept.as_mut().poll(cx);
                    if result.is_pending() {
                        entered.notify_one();
                    }
                    result
                })
                .await
            }
        );
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionAborted);
        assert!(!control
            .wait_closed(Instant::now())
            .await
            .unwrap()
            .is_complete());
        assert_bound(address);
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        std::net::TcpListener::bind(address).unwrap();
    })
    .await
    .unwrap();
}
