use super::*;
use crate::contract::lifecycle::ChannelControl;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket},
    time::timeout,
};
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn prepare(
    peer: SocketAddr,
    parent: CancellationToken,
    observer: Option<Arc<dyn IoObserver>>,
) -> (PendingTcpConnect, ControlledTcpControl) {
    let policy = zero_traits::DialPolicy {
        source_ip: Some("127.0.0.1".parse().unwrap()),
        ..Default::default()
    };
    let selected = crate::EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &policy)
        .unwrap();
    PendingTcpConnect::prepare(peer, &policy, &selected, parent, observer).unwrap()
}
#[derive(Debug, Default)]
struct Observer {
    sent: std::sync::atomic::AtomicUsize,
    received: std::sync::atomic::AtomicUsize,
}
impl IoObserver for Observer {
    fn stream_boundary(&self) {}
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
async fn pending_tcp_connect_transfers_same_native_io_and_preserves_reverse_half_close() {
    timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let observer = Arc::new(Observer::default());
        let (mut owner, mut control) =
            prepare(peer_addr, CancellationToken::new(), Some(observer.clone()));
        assert_eq!(
            owner.local_addr().unwrap().ip(),
            "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
        );
        let mut io = owner.connect(deadline()).await.unwrap();
        let (mut peer, actual_source) = listener.accept().await.unwrap();
        assert_eq!(actual_source, io.local_addr().unwrap());
        assert_eq!(io.peer_addr().unwrap(), peer_addr);
        // Dropping the pre-connect view after successful transfer must not
        // cancel the byte stream now owned by its consumer.
        drop(owner);
        io.write_all(b"request").await.unwrap();
        io.shutdown().await.unwrap();
        let mut request = Vec::new();
        peer.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"request");
        peer.write_all(b"reply").await.unwrap();
        peer.shutdown().await.unwrap();
        let mut reply = Vec::new();
        io.read_to_end(&mut reply).await.unwrap();
        assert_eq!(reply, b"reply");
        assert_eq!(observer.sent.load(std::sync::atomic::Ordering::Relaxed), 7);
        assert_eq!(
            observer.received.load(std::sync::atomic::Ordering::Relaxed),
            5
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
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn pending_tcp_connect_keeps_original_refused_os_error_and_bound_source_until_close() {
    timeout(Duration::from_secs(3), async {
        // Close the reserved endpoint to get a real refusal on every platform;
        // macOS drops SYN while a non-listening socket remains bound.
        let reserved = TcpSocket::new_v4().unwrap();
        reserved.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let refused = reserved.local_addr().unwrap();
        drop(reserved);
        let (mut owner, mut control) = prepare(refused, CancellationToken::new(), None);
        let first = owner.connect(deadline()).await.err().unwrap();
        assert_eq!(first.stage(), "connect_socket");
        assert_eq!(first.error().raw_os_error(), Some(libc::ECONNREFUSED));
        assert_eq!(first.local_addr(), Some(owner.local_addr().unwrap()));
        let repeated = owner.connect(deadline()).await.err().unwrap();
        assert!(
            Arc::ptr_eq(&first, &repeated),
            "original error retained without consuming SO_ERROR again"
        );
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert!(owner.local_addr().is_err());
        assert_eq!(first.error().raw_os_error(), Some(libc::ECONNREFUSED));
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn pending_tcp_connect_expired_deadline_preserves_unstarted_owner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let parent = CancellationToken::new();
    let (mut owner, mut control) = prepare(listener.local_addr().unwrap(), parent.clone(), None);
    let original_source = owner.local_addr().unwrap();
    let source = owner.connect(Instant::now()).await.err().unwrap();
    assert_eq!(source.error().kind(), io::ErrorKind::TimedOut);
    assert!(!owner.started);
    assert_eq!(owner.local_addr().unwrap(), original_source);
    assert!(!parent.is_cancelled());
    let report = control.wait_closed(Instant::now()).await.unwrap();
    assert!(!report.is_complete());
    assert_eq!(owner.local_addr().unwrap(), original_source);
    assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
}
#[tokio::test]
async fn pending_tcp_connect_native_pending_read_is_woken_by_cancel_and_peer_gets_eof_after_close()
{
    timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let parent = CancellationToken::new();
        let (mut owner, mut control) =
            prepare(listener.local_addr().unwrap(), parent.clone(), None);
        let mut io = owner.connect(deadline()).await.unwrap();
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
        control.cancel(crate::contract::CloseReason::Shutdown);
        let (_io, result) = reader.await.unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionAborted);
        assert!(!parent.is_cancelled());
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn pending_tcp_connect_ipv6_source_and_actual_peer_are_preserved() {
    timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("[::1]:0")
            .await
            .expect("native TCP acceptance requires IPv6 loopback");
        let peer = listener.local_addr().unwrap();
        let policy = zero_traits::DialPolicy {
            source_ip: Some("::1".parse().unwrap()),
            ..Default::default()
        };
        let selection = crate::EgressInterfaceControl::default()
            .select_for_peer_with_policy(peer, &policy)
            .unwrap();
        let (mut owner, mut control) =
            PendingTcpConnect::prepare(peer, &policy, &selection, CancellationToken::new(), None)
                .unwrap();
        let mut io = owner.connect(deadline()).await.unwrap();
        let (mut server, actual_source) = listener.accept().await.unwrap();
        assert_eq!(io.local_addr().unwrap(), actual_source);
        assert_eq!(io.peer_addr().unwrap(), peer);
        assert_eq!(actual_source.ip(), policy.source_ip.unwrap());
        io.write_all(b"6").await.unwrap();
        let mut byte = [0; 1];
        server.read_exact(&mut byte).await.unwrap();
        assert_eq!(&byte, b"6");
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert_eq!(server.read(&mut byte).await.unwrap(), 0);
    })
    .await
    .unwrap();
}
#[cfg(target_os = "linux")]
#[path = "connect_pending/backlog.rs"]
mod backlog;
