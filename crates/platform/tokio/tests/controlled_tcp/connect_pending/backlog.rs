use super::*;
use crate::contract::CloseReason;
use std::{
    future::Future,
    os::fd::{AsRawFd, RawFd},
};
fn identity(fd: RawFd) -> Option<(libc::dev_t, libc::ino_t)> {
    loop {
        let mut stat = std::mem::MaybeUninit::uninit();
        // SAFETY: fstat borrows a numeric fd, never duplicates or closes it.
        if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } == 0 {
            let stat = unsafe { stat.assume_init() };
            return Some((stat.st_dev, stat.st_ino));
        }
        let source = io::Error::last_os_error();
        if source.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        assert_eq!(source.raw_os_error(), Some(libc::EBADF));
        return None;
    }
}
fn assert_connect_waiting(fd: RawFd) {
    let mut info = std::mem::MaybeUninit::<libc::tcp_info>::zeroed();
    let mut length = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
    // SAFETY: the owner's live fd is borrowed. TCP_INFO writes at most length
    // bytes into a zero-initialized tcp_info; no ownership or SO_ERROR changes.
    assert_eq!(
        unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_TCP,
                libc::TCP_INFO,
                info.as_mut_ptr().cast(),
                &mut length,
            )
        },
        0
    );
    let info = unsafe { info.assume_init() };
    // Linux UAPI TCP_SYN_SENT = 2. This actual kernel state distinguishes a
    // pending handshake from a connected socket awaiting its first reactor tick.
    assert_eq!(
        info.tcpi_state, 2,
        "backlog saturation must leave the native socket in TCP_SYN_SENT"
    );
}
fn owned_fd(owner: &PendingTcpConnect) -> RawFd {
    let state = owner.shared.state.lock().unwrap();
    let Some(Socket::Native(socket)) = state.socket.as_ref() else {
        panic!("native owner required")
    };
    socket.original_fd()
}
async fn saturated() -> (TcpListener, tokio::net::TcpStream) {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    // Linux permits one established entry for listen(0). Keep it queued and
    // never accept it: the next SYN cannot finish its server handshake.
    let listener = socket.listen(0).unwrap();
    let filler = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    loop {
        let mut event = libc::pollfd {
            fd: listener.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll only borrows this live listener and writable event.
        let ready = unsafe { libc::poll(&mut event, 1, 0) };
        assert!(ready >= 0, "listener readiness probe failed");
        if ready > 0 {
            assert_ne!(event.revents & libc::POLLIN, 0);
            break;
        }
        tokio::task::yield_now().await;
    }
    (listener, filler)
}
#[tokio::test]
async fn pending_tcp_connect_actual_backlog_wait_drop_retains_same_fd_and_terminal_failure() {
    timeout(Duration::from_secs(3), async {
        let (listener, _filler) = saturated().await;
        let parent = CancellationToken::new();
        let (mut owner, mut control) =
            prepare(listener.local_addr().unwrap(), parent.clone(), None);
        let fd = owned_fd(&owner);
        let original = identity(fd).unwrap();
        {
            let mut connecting = Box::pin(owner.connect(deadline()));
            poll_fn(|cx| {
                assert!(
                    connecting.as_mut().poll(cx).is_pending(),
                    "backlog fixture must actually wait for native connect readiness"
                );
                Poll::Ready(())
            })
            .await;
            assert_connect_waiting(fd);
        }
        assert!(owner.started);
        assert_eq!(
            identity(fd),
            Some(original),
            "dropping connect does not close its original fd"
        );
        assert!(!parent.is_cancelled());
        let first = owner.connect(deadline()).await.err().unwrap();
        assert_eq!(first.error().kind(), io::ErrorKind::ConnectionAborted);
        let again = owner.connect(deadline()).await.err().unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        assert!(!control
            .wait_closed(Instant::now())
            .await
            .unwrap()
            .is_complete());
        assert_eq!(identity(fd), Some(original));
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert_ne!(identity(fd), Some(original));
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn pending_tcp_connect_actual_backlog_cancel_wakes_waiter_and_timeout_retains_fd() {
    timeout(Duration::from_secs(3), async {
        let (listener, _filler) = saturated().await;
        let parent = CancellationToken::new();
        let (mut owner, mut control) =
            prepare(listener.local_addr().unwrap(), parent.clone(), None);
        let fd = owned_fd(&owner);
        let original = identity(fd).unwrap();
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
        assert_connect_waiting(fd);
        control.cancel(CloseReason::Shutdown);
        let (_owner, result) = waiter.await.unwrap();
        let source = result.err().unwrap();
        assert_eq!(source.error().kind(), io::ErrorKind::ConnectionAborted);
        assert!(!parent.is_cancelled());
        assert_eq!(identity(fd), Some(original));
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert_ne!(identity(fd), Some(original));

        let (mut owner, mut control) = prepare(listener.local_addr().unwrap(), parent, None);
        let fd = owned_fd(&owner);
        let original = identity(fd).unwrap();
        let mut connecting = Box::pin(owner.connect(Instant::now() + Duration::from_millis(20)));
        poll_fn(|cx| {
            assert!(connecting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_connect_waiting(fd);
        let source = connecting.await.err().unwrap();
        assert_eq!(source.error().kind(), io::ErrorKind::TimedOut);
        assert_eq!(identity(fd), Some(original));
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert_ne!(identity(fd), Some(original));
    })
    .await
    .unwrap();
}
