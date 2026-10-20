//! Real Winsock cases; compiling these is not evidence that Windows ran them.
use super::*;
use crate::contract::{lifecycle::ChannelControl, CloseReason};
use std::{future::Future, os::windows::io::RawSocket, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket},
    time::timeout,
};
use windows_sys::Win32::Networking::WinSock::WSAECONNREFUSED;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn prepare(
    peer: SocketAddr,
    parent: CancellationToken,
    observer: Option<Arc<dyn IoObserver>>,
) -> (PendingTcpConnect, ControlledTcpControl) {
    let policy = zero_traits::DialPolicy {
        source_ip: Some(
            if peer.is_ipv4() { "127.0.0.1" } else { "::1" }
                .parse()
                .unwrap(),
        ),
        ..Default::default()
    };
    let selection = crate::EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &policy)
        .unwrap();
    PendingTcpConnect::prepare(peer, &policy, &selection, parent, observer).unwrap()
}
fn original_socket(owner: &PendingTcpConnect) -> RawSocket {
    let state = owner.shared.state();
    let Some(Socket::Native(socket)) = state.socket.as_ref() else {
        panic!("native socket required")
    };
    socket.original_socket().unwrap()
}
#[derive(Debug, Default)]
struct Observer {
    sent: std::sync::atomic::AtomicUsize,
    received: std::sync::atomic::AtomicUsize,
    boundaries: std::sync::atomic::AtomicUsize,
}
impl IoObserver for Observer {
    fn stream_boundary(&self) {
        self.boundaries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn sent(&self, n: usize) {
        self.sent.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }
    fn received(&self, n: usize) {
        self.received
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn windows_pending_tcp_connect_same_socket_source_observer_and_reverse_half_close() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let observer = Arc::new(Observer::default());
        let (mut owner, mut control) = prepare(
            listener.local_addr().unwrap(),
            CancellationToken::new(),
            Some(observer.clone()),
        );
        let identity = original_socket(&owner);
        let mut io = owner.connect(deadline()).await.unwrap();
        assert_eq!(
            original_socket(&owner),
            identity,
            "registration transfers the same original SOCKET"
        );
        let (mut peer, source) = listener.accept().await.unwrap();
        assert_eq!(io.local_addr().unwrap(), source);
        assert_eq!(
            source.ip(),
            "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
        );
        drop(owner);
        io.write_all(b"request").await.unwrap();
        io.shutdown().await.unwrap();
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"request");
        peer.write_all(b"reply").await.unwrap();
        peer.shutdown().await.unwrap();
        bytes.clear();
        io.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"reply");
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(observer.sent.load(Relaxed), 7);
        assert_eq!(observer.received.load(Relaxed), 5);
        assert_eq!(observer.boundaries.load(Relaxed), 1);
        assert_eq!(
            control
                .wait_closed(deadline())
                .await
                .unwrap()
                .closed_io
                .len(),
            1
        );
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn windows_pending_tcp_connect_refused_keeps_original_winsock_error_and_source() {
    timeout(Duration::from_secs(5), async {
        let reserved = TcpSocket::new_v4().unwrap();
        reserved.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let (mut owner, mut control) = prepare(
            reserved.local_addr().unwrap(),
            CancellationToken::new(),
            None,
        );
        let identity = original_socket(&owner);
        let source = owner.connect(deadline()).await.err().unwrap();
        assert_eq!(source.error().raw_os_error(), Some(WSAECONNREFUSED));
        assert_eq!(source.stage(), "connect_socket");
        assert_eq!(source.local_addr(), Some(owner.local_addr().unwrap()));
        assert_eq!(original_socket(&owner), identity);
        let repeated = owner.connect(deadline()).await.err().unwrap();
        assert!(Arc::ptr_eq(&source, &repeated));
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert_eq!(source.error().raw_os_error(), Some(WSAECONNREFUSED));
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn windows_pending_tcp_connect_expired_unstarted_owner_is_still_closable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let parent = CancellationToken::new();
    let (mut owner, mut control) = prepare(listener.local_addr().unwrap(), parent.clone(), None);
    let identity = original_socket(&owner);
    let source = owner.connect(Instant::now()).await.err().unwrap();
    assert_eq!(source.error().kind(), io::ErrorKind::TimedOut);
    assert!(!owner.started);
    assert_eq!(original_socket(&owner), identity);
    assert!(!parent.is_cancelled());
    assert!(!control
        .wait_closed(Instant::now())
        .await
        .unwrap()
        .is_complete());
    assert_eq!(original_socket(&owner), identity);
    assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
}
#[tokio::test]
async fn windows_pending_tcp_connect_ipv6_source_and_peer_eof_after_explicit_close() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("[::1]:0")
            .await
            .expect("Windows native acceptance requires IPv6 loopback");
        let (mut owner, mut control) = prepare(
            listener.local_addr().unwrap(),
            CancellationToken::new(),
            None,
        );
        let mut io = owner.connect(deadline()).await.unwrap();
        let (mut peer, source) = listener.accept().await.unwrap();
        assert_eq!(io.local_addr().unwrap(), source);
        assert_eq!(source.ip(), "::1".parse::<std::net::IpAddr>().unwrap());
        io.write_all(b"6").await.unwrap();
        let mut bytes = [0; 1];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"6");
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut bytes).await.unwrap(), 0);
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn windows_pending_tcp_connect_actual_pending_read_cancel_keeps_original_until_peer_eof() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let parent = CancellationToken::new();
        let (mut owner, mut control) =
            prepare(listener.local_addr().unwrap(), parent.clone(), None);
        let mut io = owner.connect(deadline()).await.unwrap();
        let original = original_socket(&owner);
        let (mut peer, _) = listener.accept().await.unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let entering = entered.clone();
        let reader = tokio::spawn(async move {
            let mut bytes = [0; 1];
            let result = poll_fn(|cx| {
                let mut buffer = tokio::io::ReadBuf::new(&mut bytes);
                let result =
                    tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut io), cx, &mut buffer);
                if result.is_pending() {
                    entering.notify_one();
                }
                result
            })
            .await;
            (io, result)
        });
        entered.notified().await;
        assert!(!reader.is_finished());
        control.cancel(CloseReason::Shutdown);
        let (_io, result) = reader.await.unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionAborted);
        assert!(!parent.is_cancelled());
        assert_eq!(original_socket(&owner), original);
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            control
                .wait_closed(deadline())
                .await
                .unwrap()
                .closed_io
                .len(),
            1
        );
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    })
    .await
    .unwrap();
}
#[path = "connect_windows/backlog.rs"]
mod backlog;
