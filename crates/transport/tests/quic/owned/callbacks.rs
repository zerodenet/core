use super::*;
use crate::quic::owned::callbacks::Boundary;
use std::{
    fmt,
    future::Future,
    panic::{catch_unwind, panic_any, AssertUnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Weak,
    },
    task::{Context, Wake, Waker},
};
#[derive(Debug)]
struct PanicTag(u32);
struct Panics {
    socket: Weak<dyn quinn::AsyncUdpSocket>,
    target: std::net::SocketAddr,
    primary: Arc<PanicTag>,
    secondary: Arc<PanicTag>,
    calls: AtomicUsize,
    nested: bool,
}
impl fmt::Debug for Panics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalPanicObserver")
            .finish_non_exhaustive()
    }
}
impl zero_traits::IoObserver for Panics {
    fn received(&self, _bytes: usize) {}
    fn sent(&self, _bytes: usize) {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.nested && call == 0 {
            self.socket
                .upgrade()
                .unwrap()
                .try_send(&quinn::udp::Transmit {
                    destination: self.target,
                    contents: b"nested",
                    ecn: None,
                    segment_size: None,
                    src_ip: None,
                })
                .unwrap();
            panic_any(self.secondary.clone());
        }
        panic_any(self.primary.clone());
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
fn transmit(target: std::net::SocketAddr) -> quinn::udp::Transmit<'static> {
    quinn::udp::Transmit {
        destination: target,
        contents: b"outer",
        ecn: None,
        segment_size: None,
        src_ip: None,
    }
}
fn nested_socket() -> MetadataSocketOwner {
    let mut owner = MetadataOwner::new(MetadataLimits {
        main_sockets: bound(1),
        probe_sockets: bound(1),
        probe_attempts_per_initialization: bound(4),
        concurrent_send_calls: bound(2),
        write_pollers: bound(2),
    })
    .unwrap();
    let grant = owner.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    MetadataSocketOwner::new(owner, grant).unwrap()
}
async fn writable(socket: &Arc<dyn quinn::AsyncUdpSocket>) {
    let mut poller = socket.clone().create_io_poller();
    poll_fn(|cx| poller.as_mut().poll_writable(cx))
        .await
        .unwrap();
}
#[tokio::test]
async fn outside_owned_driver_boundary_original_observer_payload_resumes_without_native_close() {
    let mut native = socket();
    let socket = native.socket();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let tag = Arc::new(PanicTag(1));
    let observer = Arc::new(Panics {
        socket: Arc::downgrade(&socket),
        target: peer.local_addr().unwrap(),
        primary: tag.clone(),
        secondary: tag.clone(),
        calls: AtomicUsize::new(0),
        nested: false,
    });
    native.set_observer(Some(observer.clone())).unwrap();
    writable(&socket).await;
    let payload = catch_unwind(AssertUnwindSafe(|| {
        socket.try_send(&transmit(peer.local_addr().unwrap()))
    }))
    .unwrap_err();
    let original = payload.downcast::<Arc<PanicTag>>().unwrap();
    assert!(Arc::ptr_eq(&original, &tag));
    assert_eq!(original.0, 1);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    assert!(native.observer_faulted());
    assert!(socket.local_addr().is_ok());
    let mut wire = [0; 16];
    assert_eq!(peer.recv_from(&mut wire).await.unwrap().0, 5);
    assert_eq!(native.close_io().main.confirmed_closed, 1);
}
#[tokio::test]
async fn actual_native_nested_callbacks_keep_first_original_payload_and_do_not_drop_second_during_resume(
) {
    let mut native = nested_socket();
    let socket = native.socket();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let primary = Arc::new(PanicTag(11));
    let secondary = Arc::new(PanicTag(22));
    let target = peer.local_addr().unwrap();
    let observer = Arc::new(Panics {
        socket: Arc::downgrade(&socket),
        target,
        primary: primary.clone(),
        secondary: secondary.clone(),
        calls: AtomicUsize::new(0),
        nested: true,
    });
    native.set_observer(Some(observer.clone())).unwrap();
    writable(&socket).await;
    let sending = socket.clone();
    // This isolates the original native-call poll boundary; the real QUIC
    // handshake/driver and hosted panic paths are separate actual Quinn tests.
    let future = Box::pin(async move {
        sending.try_send(&transmit(target)).unwrap();
        quinn::DriverExit::Endpoint(Ok(()))
    });
    let mut boundary = Box::pin(Boundary::new(future, native.callbacks.clone()));
    let waker = Waker::from(Arc::new(Noop));
    let payload = catch_unwind(AssertUnwindSafe(|| {
        boundary.as_mut().poll(&mut Context::from_waker(&waker))
    }))
    .unwrap_err();
    let original = payload.downcast::<Arc<PanicTag>>().unwrap();
    assert!(Arc::ptr_eq(&original, &primary));
    assert_eq!(original.0, 11);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 2);
    assert_eq!(native.suppressed_observer_panics(), 1);
    assert_eq!(
        Arc::strong_count(&secondary),
        3,
        "secondary original stays owned instead of dropping during primary resume"
    );
    let mut payload = [0; 32];
    assert_eq!(peer.recv_from(&mut payload).await.unwrap().0, 5);
    assert_eq!(peer.recv_from(&mut payload).await.unwrap().0, 6);
    assert_eq!(native.close_io().main.confirmed_closed, 1);
    assert_eq!(native.suppressed_observer_panics(), 1);
    let received = native
        .take_suppressed_observer_panic()
        .unwrap()
        .downcast::<Arc<PanicTag>>()
        .unwrap();
    assert!(Arc::ptr_eq(&received, &secondary));
    assert_eq!(received.0, 22);
    assert_eq!(native.suppressed_observer_panics(), 0);
    assert!(!native.observer_coverage_lost());
}
#[tokio::test]
async fn actual_foreign_metadata_driver_owner_refuses_native_io_before_observer_or_submission() {
    use zero_platform_tokio::contract::scope::TaskScope;
    let mut host = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let (drivers, mut receiver) = QuinnDriverOwner::new(host.registrar(), bound(1));
    let mut owner = socket();
    let mut foreign = socket();
    let endpoint = quinn::Endpoint::new_with_abstract_socket(
        Default::default(),
        None,
        foreign.socket(),
        drivers.runtime(&owner),
    )
    .unwrap();
    let exit = host.next_exit().await.unwrap();
    assert!(exit.result.is_err());
    let body = drivers.body(exit.id).unwrap();
    let source = body.io_error.as_ref().unwrap();
    assert_eq!(source.kind(), io::ErrorKind::InvalidInput);
    let mut error: &(dyn std::error::Error + 'static) = exit.result.as_ref().unwrap_err();
    loop {
        if let Some(actual) = error.downcast_ref::<io::Error>() {
            assert!(
                std::ptr::eq(actual, source.as_ref()),
                "task source is the same original error Arc"
            );
            break;
        }
        error = error
            .source()
            .expect("original I/O source survives actual join");
    }
    receiver
        .receive(
            exit.id,
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(!foreign.observer_faulted());
    assert!(owner.socket().local_addr().is_ok());
    assert!(foreign.socket().local_addr().is_ok());
    drop(endpoint);
    assert_eq!(owner.close_io().main.confirmed_closed, 1);
    assert_eq!(foreign.close_io().main.confirmed_closed, 1);
}

#[tokio::test]
async fn actual_incoming_refusal_resumes_original_observer_panic_after_quinn_state_unlock() {
    use zero_platform_tokio::contract::scope::TaskScope;
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(2));
    let (drivers, mut receiver) = QuinnDriverOwner::new(scope.registrar(), bound(2));
    let mut native = socket();
    let endpoint = super::drivers::endpoint(&drivers, &native);
    let target = endpoint.local_addr().unwrap();
    let connecting = endpoint.connect(target, "localhost").unwrap();
    let incoming = tokio::time::timeout(std::time::Duration::from_secs(2), endpoint.accept())
        .await
        .unwrap()
        .unwrap();
    let socket = native.socket();
    writable(&socket).await;
    let original = Arc::new(PanicTag(33));
    let observer = Arc::new(Panics {
        socket: Arc::downgrade(&socket),
        target,
        primary: original.clone(),
        secondary: original.clone(),
        calls: AtomicUsize::new(0),
        nested: false,
    });
    native.set_observer(Some(observer.clone())).unwrap();
    let payload = catch_unwind(AssertUnwindSafe(|| {
        // Incoming::Drop submits the real CONNECTION_REFUSED under Quinn's
        // Endpoint State mutex. A poisoned state would panic again in stats or
        // the original endpoint/connecting destructors below.
        native.callbacks.call(|| drop(incoming));
    }))
    .unwrap_err()
    .downcast::<Arc<PanicTag>>()
    .unwrap();
    assert!(Arc::ptr_eq(&payload, &original));
    assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
    assert_eq!(endpoint.stats().refused_handshakes, 1);
    assert!(native.observer_faulted());
    assert!(
        socket.local_addr().is_ok(),
        "a user panic is no native receipt"
    );
    assert_eq!(drivers.task_status().unjoined.len(), 2);
    endpoint.close(0u32.into(), b"test cleanup");
    drop(connecting);
    drop(endpoint);
    for _ in 0..2 {
        let exit = tokio::time::timeout(std::time::Duration::from_secs(3), scope.next_exit())
            .await
            .unwrap()
            .unwrap();
        exit.result.unwrap();
        receiver
            .receive(
                exit.id,
                tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
            .unwrap();
    }
    assert_eq!(native.close_io().main.confirmed_closed, 1);
}
