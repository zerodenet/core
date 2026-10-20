use super::*;
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawSocket;

#[tokio::test]
async fn consuming_platform_listener_keeps_same_native_handle_and_port_until_confirmed_close() {
    timeout(Duration::from_secs(3), async {
        let original = crate::TokioListener::bind_addr("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address = original.local_addr().unwrap();
        #[cfg(unix)]
        let original_handle = original.inner.as_raw_fd();
        #[cfg(windows)]
        let original_handle = original.inner.as_raw_socket();
        let parent = CancellationToken::new();
        let (view, mut control) = original.into_controlled(parent.clone());
        assert_eq!(view.local_addr().unwrap(), address);
        {
            let state = view.shared.state.lock().unwrap();
            let actual = state.listener.as_ref().unwrap();
            #[cfg(unix)]
            assert_eq!(actual.as_raw_fd(), original_handle);
            #[cfg(windows)]
            assert_eq!(actual.as_raw_socket(), original_handle);
        }
        assert_bound(address);
        drop(view);
        assert_bound(address);
        assert!(!parent.is_cancelled());
        drop(control.wait_closed(deadline())); // no poll, no native close
        assert_bound(address);
        let expired = control.wait_closed(Instant::now()).await.unwrap();
        assert!(!expired.is_complete());
        assert!(expired.closed_io.is_empty());
        assert_bound(address);
        let report = control.wait_closed(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        let rebound = std::net::TcpListener::bind(address).unwrap();
        assert_eq!(rebound.local_addr().unwrap(), address);
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
async fn converted_listener_accept_can_be_abandoned_and_resume_before_explicit_close() {
    timeout(Duration::from_secs(3), async {
        let original = crate::TokioListener::bind_addr("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address = original.local_addr().unwrap();
        let parent = CancellationToken::new();
        let (mut view, mut control) = original.into_controlled(parent.clone());
        {
            let mut pending = Box::pin(view.accept());
            poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_bound(address);
        let (client, accepted) = tokio::join!(TcpStream::connect(address), view.accept());
        let mut client = client.unwrap();
        let (mut accepted, remote) = accepted.unwrap();
        assert_eq!(remote, client.local_addr().unwrap());
        assert_eq!(accepted.local_addr().unwrap(), address);
        let report = control.wait_closed(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert!(!parent.is_cancelled());
        assert_eq!(
            view.local_addr().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            view.accept().await.unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        // The original listener receipt never covers accepted caller-owned I/O.
        // Retained children remain usable after that exact listener is closed.
        client.write_all(b"request").await.unwrap();
        let mut request = [0; 7];
        accepted.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        accepted.write_all(b"reply").await.unwrap();
        let mut reply = [0; 5];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
        drop((client, accepted)); // test-owned children; no child close receipt
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn converted_listener_parent_cancel_wakes_real_pending_accept_and_keeps_native_owner() {
    timeout(Duration::from_secs(3), async {
        let original = crate::TokioListener::bind_addr("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address = original.local_addr().unwrap();
        let parent = CancellationToken::new();
        let (mut view, mut control) = original.into_controlled(parent.clone());
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
        assert_bound(address);
        let expired = control.wait_closed(Instant::now()).await.unwrap();
        assert!(!expired.is_complete());
        assert!(expired.closed_io.is_empty());
        assert_bound(address);
        let report = control.wait_closed(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert_eq!(
            view.accept().await.unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        let rebound = std::net::TcpListener::bind(address).unwrap();
        assert_eq!(rebound.local_addr().unwrap(), address);
    })
    .await
    .unwrap();
}
