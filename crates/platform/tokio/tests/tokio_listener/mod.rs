use super::*;
use crate::contract::{lifecycle::ChannelControl, CloseReason};
use std::{future::poll_fn, future::Future, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{timeout, Instant},
};

#[tokio::test]
async fn controlled_platform_view_keeps_the_same_owner_across_cancelled_accept_and_close() {
    timeout(Duration::from_secs(3), async {
        let original = TokioListener::bind_addr("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address = original.local_addr().unwrap();
        let (view, mut control) = original.into_controlled(CancellationToken::new());
        let listener = TokioListener::from_controlled(view).unwrap();
        {
            let mut accepting = Box::pin(listener.accept());
            poll_fn(|cx| {
                assert!(accepting.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_eq!(listener.local_addr().unwrap(), address);
        }
        // The canceled accept releases its mutex guard and native waker; it
        // does not replace the listener or retire the unique accept view.
        let (peer, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let mut peer = peer.unwrap();
        let (mut accepted, remote) = accepted.unwrap();
        assert_eq!(
            remote,
            Some(socket_addr_to_socket_address(peer.local_addr().unwrap()))
        );
        peer.write_all(b"before").await.unwrap();
        let mut first = [0; 6];
        accepted.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"before");
        let entered = tokio::sync::Notify::new();
        let ((), pending) = tokio::join!(
            async {
                entered.notified().await;
                control.cancel(CloseReason::Shutdown);
            },
            async {
                let mut accepting = Box::pin(listener.accept());
                poll_fn(|cx| {
                    let result = accepting.as_mut().poll(cx);
                    if result.is_pending() {
                        entered.notify_one();
                    }
                    result
                })
                .await
            }
        );
        assert_eq!(
            pending.unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            std::net::TcpListener::bind(address).unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );
        let report = control
            .wait_closed(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        let rebound = std::net::TcpListener::bind(address).unwrap();
        assert_eq!(rebound.local_addr().unwrap(), address);
        // Accepted stream ownership is independent from the listening socket.
        peer.write_all(b"after").await.unwrap();
        let mut second = [0; 5];
        accepted.read_exact(&mut second).await.unwrap();
        assert_eq!(&second, b"after");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn controlled_view_cannot_extract_a_second_native_control_and_remains_usable() {
    timeout(Duration::from_secs(3), async {
        let original = TokioListener::bind_addr("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address = original.local_addr().unwrap();
        let (view, mut control) = original.into_controlled(CancellationToken::new());
        let listener = TokioListener::from_controlled(view).unwrap();
        let listener = match listener.try_into_controlled(CancellationToken::new()) {
            Err(original_view) => original_view,
            Ok(_) => panic!("an accept view must not duplicate its owner's close authority"),
        };
        let (peer, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let _peer = peer.unwrap();
        let (_accepted, _) = accepted.unwrap();
        drop(listener);
        assert_eq!(
            std::net::TcpListener::bind(address).unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );
        assert!(control
            .wait_closed(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        let _rebound = std::net::TcpListener::bind(address).unwrap();
    })
    .await
    .unwrap();
}
