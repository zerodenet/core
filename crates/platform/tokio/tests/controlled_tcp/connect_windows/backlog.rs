//! Bounded local backlog fixture. It must establish actual SYN_SENT; a provider
//! that refuses full queues does not qualify as a pending-connect test fixture.
use super::*;
#[path = "backlog/native_state.rs"]
mod native_state;
use native_state::assert_connect_waiting;
struct Fixture {
    _listener: TcpListener,
    fillers: Vec<(PendingTcpConnect, ControlledTcpControl)>,
}
impl Fixture {
    async fn close(self) {
        for (_owner, mut control) in self.fillers {
            assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        }
    }
}
async fn saturated(
    parent: CancellationToken,
) -> (Fixture, PendingTcpConnect, ControlledTcpControl) {
    let listener = TcpSocket::new_v4().unwrap();
    listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = listener.listen(1).unwrap();
    let peer = listener.local_addr().unwrap();
    let mut fixture = Fixture {
        _listener: listener,
        fillers: Vec::new(),
    };
    // Windows providers may adjust backlog sizes. Retain each actual completed
    // connection without accepting, and stop only at an observed native wait.
    for _ in 0..64 {
        let (mut owner, mut control) = prepare(peer, parent.clone(), None);
        match timeout(
            Duration::from_millis(50),
            poll_fn(|cx| owner.poll_connect(cx)),
        )
        .await
        {
            Ok(Ok(())) => fixture.fillers.push((owner, control)),
            Ok(Err(source)) => {
                // In particular WSAECONNREFUSED is NOT a pending-connect proof.
                assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
                fixture.close().await;
                panic!("Windows provider did not supply a pending backlog fixture: {source:?}");
            }
            Err(_) => {
                assert!(owner.started);
                assert_connect_waiting(owner.local_addr().unwrap(), owner.peer);
                return (fixture, owner, control);
            }
        }
    }
    fixture.close().await;
    panic!("bounded Windows backlog fixture never produced actual pending connect");
}
#[tokio::test]
async fn windows_pending_tcp_connect_backlog_drop_retains_same_socket_until_explicit_close() {
    timeout(Duration::from_secs(10), async {
        let parent = CancellationToken::new();
        let (fixture, mut owner, mut control) = saturated(parent.clone()).await;
        let original = original_socket(&owner);
        let local = owner.local_addr().unwrap();
        let peer = owner.peer;
        {
            let mut connecting = Box::pin(owner.connect(deadline()));
            poll_fn(|cx| {
                assert!(connecting.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_connect_waiting(local, peer);
        }
        assert!(!parent.is_cancelled());
        assert_eq!(original_socket(&owner), original);
        assert_connect_waiting(local, peer);
        let source = owner.connect(deadline()).await.err().unwrap();
        assert_eq!(source.error().kind(), io::ErrorKind::ConnectionAborted);
        assert!(Arc::ptr_eq(
            &source,
            &owner.connect(deadline()).await.err().unwrap()
        ));
        assert!(!control
            .wait_closed(Instant::now())
            .await
            .unwrap()
            .is_complete());
        assert_eq!(original_socket(&owner), original);
        assert_eq!(
            control
                .wait_closed(deadline())
                .await
                .unwrap()
                .closed_io
                .len(),
            1
        );
        assert!(owner.local_addr().is_err());
        fixture.close().await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn windows_pending_tcp_connect_backlog_cancel_wakes_actual_waiter() {
    timeout(Duration::from_secs(10), async {
        let parent = CancellationToken::new();
        let (fixture, mut owner, mut control) = saturated(parent.clone()).await;
        let original = original_socket(&owner);
        let local = owner.local_addr().unwrap();
        let peer = owner.peer;
        let entered = Arc::new(tokio::sync::Notify::new());
        let entering = entered.clone();
        let waiter = tokio::spawn(async move {
            let result = {
                let mut connecting = Box::pin(owner.connect(deadline()));
                poll_fn(|cx| {
                    let result = connecting.as_mut().poll(cx);
                    if result.is_pending() {
                        entering.notify_one();
                    }
                    result
                })
                .await
            };
            (owner, result)
        });
        entered.notified().await;
        assert!(!waiter.is_finished());
        assert_connect_waiting(local, peer);
        control.cancel(CloseReason::Shutdown);
        let (owner, result) = waiter.await.unwrap();
        assert_eq!(
            result.err().unwrap().error().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert!(!parent.is_cancelled());
        assert_eq!(original_socket(&owner), original);
        assert_connect_waiting(local, peer);
        assert_eq!(
            control
                .wait_closed(deadline())
                .await
                .unwrap()
                .closed_io
                .len(),
            1
        );
        fixture.close().await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn windows_pending_tcp_connect_backlog_deadline_keeps_original_handle_and_source() {
    timeout(Duration::from_secs(10), async {
        let (fixture, mut owner, mut control) = saturated(CancellationToken::new()).await;
        let original = original_socket(&owner);
        let local = owner.local_addr().unwrap();
        let peer = owner.peer;
        let mut connecting = Box::pin(owner.connect(Instant::now() + Duration::from_millis(20)));
        poll_fn(|cx| {
            assert!(connecting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_connect_waiting(local, peer);
        let source = connecting.await.err().unwrap();
        assert_eq!(source.error().kind(), io::ErrorKind::TimedOut);
        assert_eq!(source.local_addr(), Some(owner.local_addr().unwrap()));
        assert_eq!(original_socket(&owner), original);
        assert_connect_waiting(local, peer);
        assert!(!control
            .wait_closed(Instant::now())
            .await
            .unwrap()
            .is_complete());
        assert_eq!(original_socket(&owner), original);
        assert_eq!(
            control
                .wait_closed(deadline())
                .await
                .unwrap()
                .closed_io
                .len(),
            1
        );
        fixture.close().await;
    })
    .await
    .unwrap();
}
