use super::*;
use crate::runtime::Proxy;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{timeout, Duration},
};
fn proxy() -> Proxy {
    Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    (client.unwrap(), accepted.unwrap().0)
}
async fn pending<T>(work: &mut std::pin::Pin<Box<impl Future<Output = T>>>) {
    std::future::poll_fn(|cx| {
        assert!(work.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn readiness_waits_for_both_actual_loops_after_pending_initialization_and_survives_waiter_cancel(
) {
    let proxy = proxy();
    let (stop, mut stopping) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install("ready", &proxy.protocols, 1, stop)
        .unwrap();
    let owner = installed.owner();
    let mut branches = owner.readiness().split(2).unwrap().into_iter();
    let tcp_ready = branches.next().unwrap();
    let udp_ready = branches.next().unwrap();
    let (mut activation, mut gate) = tcp_pair().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_address = socket.local_addr().unwrap();
    let initializing = Arc::new(Notify::new());
    let entered = initializing.clone();
    owner.spawn_driver(async move {
        let mut byte = [0];
        let read = gate.read_exact(&mut byte);
        tokio::pin!(read);
        std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
            Poll::Pending => { entered.notify_one(); Poll::Pending }
            Poll::Ready(result) => Poll::Ready(result),
        }).await?;
        tcp_ready.poll_loop(async {
            tokio::select! {
                _ = async { let _ = stopping.wait_for(|stop| *stop).await; } => Ok(()),
                accepted = listener.accept() => { accepted?; Err(std::io::Error::other("unexpected accept").into()) },
            }
        }).await
    }).unwrap();
    let cancel = installed.tasks().cancellation();
    installed.tasks().spawn(async move {
        let mut byte = [0];
        udp_ready.poll_loop(async {
            tokio::select! {
                _ = cancel.cancelled() => Ok(()),
                received = socket.recv_from(&mut byte) => { received?; Err(std::io::Error::other("unexpected datagram").into()) },
            }
        }).await
    }).unwrap();
    let observing = {
        let owner = owner.clone();
        tokio::spawn(async move { owner.observe_driver().await })
    };
    timeout(Duration::from_secs(2), initializing.notified())
        .await
        .unwrap();
    let mut waiting = Box::pin(owner.wait_ready());
    pending(&mut waiting).await;
    drop(waiting);
    assert!(Arc::ptr_eq(
        &proxy.inbound_owners.owner("ready").unwrap(),
        &owner
    ));
    assert!(TcpListener::bind(address).await.is_err());
    assert!(UdpSocket::bind(udp_address).await.is_err());
    activation.write_all(b"r").await.unwrap();
    timeout(Duration::from_secs(2), owner.wait_ready())
        .await
        .unwrap()
        .unwrap();
    owner.task_access().request_stop();
    timeout(Duration::from_secs(2), observing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    proxy
        .inbound_owners
        .shutdown("ready", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let _tcp = TcpListener::bind(address).await.unwrap();
    let _udp = UdpSocket::bind(udp_address).await.unwrap();
    // A new real instance cannot inherit the old root's completed nonce.
    let (stop, _) = watch::channel(false);
    let next = proxy
        .inbound_owners
        .install("ready", &proxy.protocols, 0, stop)
        .unwrap()
        .owner();
    assert_ne!(owner.identity(), next.identity());
    let mut waiting = Box::pin(next.wait_ready());
    pending(&mut waiting).await;
    drop(waiting);
    proxy
        .inbound_owners
        .shutdown("ready", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn pre_ready_actual_task_panic_preserves_original_join_source_and_never_confirms() {
    let proxy = proxy();
    let (stop, _) = watch::channel(false);
    let owner = proxy
        .inbound_owners
        .install("failed", &proxy.protocols, 0, stop)
        .unwrap()
        .owner();
    owner
        .spawn_driver(async { panic!("actual pre-ready listener failure") })
        .unwrap();
    let observing = {
        let owner = owner.clone();
        tokio::spawn(async move { owner.observe_driver().await })
    };
    let failed = timeout(Duration::from_secs(2), owner.wait_ready())
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        failed
            .to_string()
            .contains("actual pre-ready listener failure"),
        "{failed:?}"
    );
    assert!(!owner.ready.0.complete.load(Ordering::Acquire));
    assert!(observing.await.unwrap().is_err());
    let original = owner.failure.lock().unwrap().clone().unwrap();
    let failures = owner.task_failures.lock().unwrap();
    let actual = failures.get(owner.driver.get().unwrap()).unwrap();
    assert!(Arc::ptr_eq(&actual.source, &original));
    let source = std::error::Error::source(original.as_ref()).unwrap();
    assert!(source
        .downcast_ref::<tokio::task::JoinError>()
        .is_some_and(|join| join.is_panic()));
    drop(failures);
    assert!(proxy
        .inbound_owners
        .shutdown("failed", Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    assert!(Arc::ptr_eq(
        owner.failure.lock().unwrap().as_ref().unwrap(),
        &original
    ));
}

struct OccupiedUdp(std::net::SocketAddr);
impl zero_platform_tokio::contract::lifecycle::ResourceLifecycle for OccupiedUdp {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        match UdpSocket::bind(self.0).await {
            Err(source) => Err(
                ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                    .with_source(source),
            ),
            Ok(_) => panic!("the actual competing UDP owner must retain its socket"),
        }
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        // This failed bind never acquired a native socket.
        Ok(CloseReport::default())
    }
}
#[tokio::test]
async fn resource_start_failure_prevents_loop_poll_and_ready_with_original_os_error() {
    let proxy = proxy();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (stop, _) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install("resource-failed", &proxy.protocols, 0, stop)
        .unwrap();
    let resource = installed
        .register(zero_platform_tokio::contract::lifecycle::erase_lifecycle(
            OccupiedUdp(socket.local_addr().unwrap()),
        ))
        .unwrap();
    let owner = installed.owner();
    owner
        .spawn_driver(async { panic!("failed resource start must not poll the listener loop") })
        .unwrap();
    let observing = {
        let owner = owner.clone();
        tokio::spawn(async move { owner.observe_driver().await })
    };
    let error = timeout(Duration::from_secs(2), owner.wait_ready())
        .await
        .unwrap()
        .unwrap_err();
    assert!(observing.await.unwrap().is_err());
    assert!(!owner.ready.0.complete.load(Ordering::Acquire));
    let original = owner.failure.lock().unwrap().clone().unwrap();
    assert_eq!(original.component, Some(resource.component));
    let source = std::error::Error::source(original.as_ref())
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(source.kind(), std::io::ErrorKind::AddrInUse);
    assert!(source.raw_os_error().is_some());
    let EngineError::Io(error) = error else {
        panic!("expected retained native error")
    };
    let retained = error
        .get_ref()
        .unwrap()
        .downcast_ref::<ChannelError>()
        .unwrap();
    let original_ref = std::error::Error::source(retained)
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap();
    assert!(Arc::ptr_eq(original_ref, &original));
    assert!(proxy
        .inbound_owners
        .shutdown("resource-failed", Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
}
